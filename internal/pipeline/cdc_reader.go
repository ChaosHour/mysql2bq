package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type CDCReader struct {
	cfg    *config.Config
	logger *logging.Logger
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

func NewCDCReader(cfg *config.Config, logger *logging.Logger) *CDCReader {
	return &CDCReader{
		cfg:    cfg,
		logger: logger,
	}
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

	// Get current binlog position (for now, start from beginning)
	// In production, this would come from checkpoint
	pos := mysql.Position{
		Name: "mysql-bin.000001", // This should be configurable or from checkpoint
		Pos:  4,                  // Skip binlog header
	}

	r.logger.Info("Starting CDC from position: %s:%d", pos.Name, pos.Pos)

	// Start streaming
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("failed to start binlog sync: %w", err)
	}

	defer syncer.Close()

	r.logger.CDCStart("CDC reader started successfully")

	for {
		select {
		case <-ctx.Done():
			r.logger.CDCComplete("CDC reader stopped by context cancellation")
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
	default:
		// Skip other event types (queries, etc.)
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

func (r *CDCReader) rowToMap(row []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for i, value := range row {
		// For now, use column index as key
		// In production, this would map to actual column names from schema
		result[fmt.Sprintf("col_%d", i)] = value
	}
	return result
}
