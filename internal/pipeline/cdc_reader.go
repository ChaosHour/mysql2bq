package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/checkpoint"
	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
)

type CDCReader struct {
	cfg        *config.Config
	logger     *logging.Logger
	checkpoint checkpoint.Store

	// Asynchronous checkpointing
	checkpointChan        chan *checkpoint.Checkpoint
	checkpointDone        chan struct{}
	lastCheckpoint        time.Time
	eventsSinceCheckpoint int64
}

func NewCDCReader(cfg *config.Config, logger *logging.Logger) *CDCReader {
	return &CDCReader{
		cfg:            cfg,
		logger:         logger,
		checkpoint:     checkpoint.NewFileStore(cfg.Checkpoint.Path),
		checkpointChan: make(chan *checkpoint.Checkpoint, 10), // Buffered channel
		checkpointDone: make(chan struct{}),
		lastCheckpoint: time.Now(),
	}
}

type CDCEvent struct {
	Database    string
	Table       string
	EventType   string
	Rows        []map[string]interface{}
	Timestamp   time.Time
	Position    mysql.Position
	Transaction string
}

// detectGTIDMode checks if MySQL is using GTID mode
func (r *CDCReader) detectGTIDMode() (bool, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql",
		r.cfg.MySQL.User, r.cfg.MySQL.Password, r.cfg.MySQL.Host, r.cfg.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	var gtidMode string
	err = db.QueryRow("SELECT @@global.gtid_mode").Scan(&gtidMode)
	if err != nil {
		return false, fmt.Errorf("failed to query GTID mode: %w", err)
	}

	return gtidMode == "ON", nil
}

// getCurrentGTIDSet retrieves the current GTID set from MySQL
func (r *CDCReader) getCurrentGTIDSet() (mysql.GTIDSet, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql",
		r.cfg.MySQL.User, r.cfg.MySQL.Password, r.cfg.MySQL.Host, r.cfg.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	var executedGTIDSet string
	err = db.QueryRow("SELECT @@global.gtid_executed").Scan(&executedGTIDSet)
	if err != nil {
		return nil, fmt.Errorf("failed to query executed GTID set: %w", err)
	}

	gtidSet, err := mysql.ParseGTIDSet("mysql", executedGTIDSet)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GTID set: %w", err)
	}

	return gtidSet, nil
}

func (r *CDCReader) Start(ctx context.Context, eventChan chan<- *CDCEvent) error {
	r.logger.CDCStart("Starting MySQL CDC reader")

	// Create MySQL connection config
	cfg := replication.BinlogSyncerConfig{
		ServerID: r.cfg.MySQL.ServerID,
		Flavor:   "mysql",
		Host:     r.cfg.MySQL.Host,
		Port:     uint16(r.cfg.MySQL.Port),
		User:     r.cfg.MySQL.User,
		Password: r.cfg.MySQL.Password,
	}

	// Create binlog syncer
	syncer := replication.NewBinlogSyncer(cfg)

	// Detect GTID mode
	gtidEnabled, err := r.detectGTIDMode()
	if err != nil {
		r.logger.Warn("Failed to detect GTID mode, falling back to position-based replication: %v", err)
		gtidEnabled = false
	}

	// Load checkpoint
	cp, err := r.checkpoint.Load()
	if err != nil {
		r.logger.Warn("Failed to load checkpoint, starting from beginning: %v", err)
		cp = nil
	}

	var streamer *replication.BinlogStreamer

	if gtidEnabled {
		r.logger.Info("MySQL GTID mode detected, using GTID-based replication")

		var gtidSet mysql.GTIDSet
		if cp != nil && cp.GTIDSet != "" {
			// Use checkpointed GTID set
			gtidSet, err = mysql.ParseGTIDSet("mysql", cp.GTIDSet)
			if err != nil {
				r.logger.Warn("Failed to parse checkpointed GTID set, getting current GTID set: %v", err)
				gtidSet, err = r.getCurrentGTIDSet()
			}
		} else {
			// Get current GTID set
			gtidSet, err = r.getCurrentGTIDSet()
		}

		if err != nil {
			r.logger.Warn("Failed to get GTID set, falling back to position-based replication: %v", err)
			gtidEnabled = false
		} else {
			r.logger.Info("Starting CDC from GTID set: %s", gtidSet.String())

			// Start streaming with GTID
			streamer, err = syncer.StartSyncGTID(gtidSet)
			if err != nil {
				// Attempt to fallback to position-based replication if GTID streaming fails
				r.logger.Warn("GTID-based sync failed: %v — attempting position-based fallback", err)

				pos, posErr := r.getCurrentBinlogPosition()
				if posErr != nil {
					return fmt.Errorf("failed to start GTID-based binlog sync: %w; fallback to position failed: %v", err, posErr)
				}

				r.logger.Info("Falling back to position-based replication starting at %s:%d", pos.Name, pos.Pos)
				streamer, err = syncer.StartSync(*pos)
				if err != nil {
					return fmt.Errorf("failed to start fallback position-based binlog sync: %w", err)
				}

				// Mark GTID as disabled for downstream checkpoint logic
				gtidEnabled = false
			}
		}
	}

	if !gtidEnabled {
		r.logger.Info("Using position-based replication")

		var pos mysql.Position
		if cp != nil && cp.Position != nil {
			// Use checkpointed position
			pos = *cp.Position
		} else {
			// Start from beginning
			pos = mysql.Position{
				Name: "mysql-bin.000001",
				Pos:  4, // Skip binlog header
			}
		}

		r.logger.Info("Starting CDC from position: %s:%d", pos.Name, pos.Pos)

		// Start streaming with position
		streamer, err = syncer.StartSync(pos)
		if err != nil {
			return fmt.Errorf("failed to start position-based binlog sync: %w", err)
		}
	}

	defer syncer.Close()

	r.logger.CDCStart("CDC reader started successfully")

	// Start asynchronous checkpoint saver
	go r.checkpointSaver(ctx)

	for {
		select {
		case <-ctx.Done():
			r.logger.CDCComplete("CDC reader stopped by context cancellation")
			close(r.checkpointChan) // Signal checkpoint saver to stop
			<-r.checkpointDone      // Wait for checkpoint saver to finish
			return ctx.Err()

		default:
			// Read next event
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				r.logger.CDCError("Failed to get binlog event", err)
				return fmt.Errorf("failed to get binlog event: %w", err)
			}

			// Process the event
			cdcEvent, err := r.processEvent(ev)
			if err != nil {
				r.logger.CDCError("Failed to process binlog event", err)
				continue // Skip this event but continue processing
			}

			if cdcEvent != nil {
				select {
				case eventChan <- cdcEvent:
					r.logger.CDCProgress("Sent CDC event to processor",
						len(cdcEvent.Rows))

					// Queue checkpoint for asynchronous saving
					r.eventsSinceCheckpoint++
					if r.shouldCheckpoint() {
						if cp := r.createCheckpoint(gtidEnabled); cp != nil {
							select {
							case r.checkpointChan <- cp:
								r.eventsSinceCheckpoint = 0
								r.lastCheckpoint = time.Now()
							default:
								r.logger.Warn("Checkpoint channel full, skipping checkpoint")
							}
						}
					}

				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

func (r *CDCReader) processEvent(ev *replication.BinlogEvent) (*CDCEvent, error) {
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return r.processRowsEvent(ev, "INSERT")
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return r.processRowsEvent(ev, "UPDATE")
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return r.processRowsEvent(ev, "DELETE")
	case replication.QUERY_EVENT:
		return r.processQueryEvent(ev)
	default:
		// Skip other event types
		return nil, nil
	}
}

func (r *CDCReader) processRowsEvent(ev *replication.BinlogEvent, eventType string) (*CDCEvent, error) {
	rowsEvent := ev.Event.(*replication.RowsEvent)

	cdcEvent := &CDCEvent{
		Database:  string(rowsEvent.Table.Schema),
		Table:     string(rowsEvent.Table.Table),
		EventType: eventType,
		Timestamp: time.Unix(int64(ev.Header.Timestamp), 0),
		Position: mysql.Position{
			Name: "", // Would need to track current binlog file
			Pos:  ev.Header.LogPos,
		},
		Rows: make([]map[string]interface{}, 0),
	}

	// Process rows based on event type
	switch eventType {
	case "INSERT":
		for _, row := range rowsEvent.Rows {
			cdcEvent.Rows = append(cdcEvent.Rows, r.rowToMap(row))
		}
	case "UPDATE":
		// UPDATE events have before/after pairs
		for i := 0; i < len(rowsEvent.Rows); i += 2 {
			// Use the "after" image (second row in pair)
			if i+1 < len(rowsEvent.Rows) {
				cdcEvent.Rows = append(cdcEvent.Rows, r.rowToMap(rowsEvent.Rows[i+1]))
			}
		}
	case "DELETE":
		for _, row := range rowsEvent.Rows {
			cdcEvent.Rows = append(cdcEvent.Rows, r.rowToMap(row))
		}
	}

	return cdcEvent, nil
}

func (r *CDCReader) processQueryEvent(ev *replication.BinlogEvent) (*CDCEvent, error) {
	queryEvent := ev.Event.(*replication.QueryEvent)
	query := string(queryEvent.Query)

	// Check for transaction boundary queries
	switch query {
	case "BEGIN", "START TRANSACTION":
		return &CDCEvent{
			EventType: "BEGIN",
			Timestamp: time.Unix(int64(ev.Header.Timestamp), 0),
			Position: mysql.Position{
				Name: "", // Would need to track current binlog file
				Pos:  ev.Header.LogPos,
			},
			Transaction: fmt.Sprintf("tx_%d", ev.Header.LogPos), // Use log position as transaction ID
		}, nil
	case "COMMIT":
		return &CDCEvent{
			EventType: "COMMIT",
			Timestamp: time.Unix(int64(ev.Header.Timestamp), 0),
			Position: mysql.Position{
				Name: "", // Would need to track current binlog file
				Pos:  ev.Header.LogPos,
			},
			Transaction: fmt.Sprintf("tx_%d", ev.Header.LogPos), // Use log position as transaction ID
		}, nil
	default:
		// Skip other queries
		return nil, nil
	}
}

func (r *CDCReader) rowToMap(row []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for i, value := range row {
		// For now, use column index as key
		// In production, this would map to actual column names from schema
		result[fmt.Sprintf("col_%d", i)] = value
	}
	return result
}

// getCurrentBinlogPosition queries MySQL for the current binlog file and position
func (r *CDCReader) getCurrentBinlogPosition() (*mysql.Position, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql",
		r.cfg.MySQL.User, r.cfg.MySQL.Password, r.cfg.MySQL.Host, r.cfg.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	var file string
	var position uint32
	err = db.QueryRow("SHOW MASTER STATUS").Scan(&file, &position, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query master status: %w", err)
	}

	return &mysql.Position{
		Name: file,
		Pos:  position,
	}, nil
}

// checkpointSaver runs in a goroutine to save checkpoints asynchronously
func (r *CDCReader) checkpointSaver(ctx context.Context) {
	defer close(r.checkpointDone)

	for {
		select {
		case <-ctx.Done():
			// Save final checkpoint before shutting down
			r.logger.Info("Saving final checkpoint before shutdown")
			return

		case cp, ok := <-r.checkpointChan:
			if !ok {
				// Channel closed, exit
				return
			}

			if err := r.checkpoint.Save(cp); err != nil {
				r.logger.Warn("Failed to save checkpoint asynchronously: %v", err)
			} else {
				r.logger.Debug("Checkpoint saved successfully")
			}
		}
	}
}

// shouldCheckpoint determines if a checkpoint should be saved
func (r *CDCReader) shouldCheckpoint() bool {
	// Checkpoint every 100 events or every 30 seconds
	return r.eventsSinceCheckpoint >= 100 || time.Since(r.lastCheckpoint) >= 30*time.Second
}

// createCheckpoint creates a checkpoint object with current position/GTID
func (r *CDCReader) createCheckpoint(gtidEnabled bool) *checkpoint.Checkpoint {
	cp := &checkpoint.Checkpoint{
		Timestamp: time.Now().Unix(),
	}

	if gtidEnabled {
		// For GTID mode, we need to get the current GTID set from MySQL
		gtidSet, err := r.getCurrentGTIDSet()
		if err != nil {
			r.logger.Warn("Failed to get current GTID set for checkpoint: %v", err)
			return nil
		}
		cp.GTIDSet = gtidSet.String()
	} else {
		// For position mode, query MySQL for current binlog file and position
		pos, err := r.getCurrentBinlogPosition()
		if err != nil {
			r.logger.Warn("Failed to get current binlog position for checkpoint: %v", err)
			return nil
		}
		cp.Position = pos
	}

	return cp
}
