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
	_ "github.com/go-sql-driver/mysql"
)

type Pipeline struct {
	cfg               *config.Config
	logger            *logging.Logger
	metrics           *CDCMetrics
	metricsServer     *MetricsServer
	transactionBuffer *TransactionBuffer
}

func New(cfg *config.Config) (*Pipeline, error) {
	// Initialize logger
	logLevel := parseLogLevel(cfg.Logging.Level)
	logger, err := logging.NewLogger(logLevel, cfg.Logging.File)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	metrics := NewCDCMetrics()

	return &Pipeline{
		cfg:               cfg,
		logger:            logger,
		metrics:           metrics,
		metricsServer:     NewMetricsServer(cfg, logger, metrics),
		transactionBuffer: NewTransactionBuffer(cfg, logger, metrics),
	}, nil
}

func (p *Pipeline) Run(ctx context.Context) error {
	p.logger.LogPipelineEvent("startup", "Pipeline initialized")
	p.logger.Info("Tables configured: %d", len(p.cfg.CDC.Tables))
	p.logger.Info("Pipeline ready - waiting for MySQL CDC events")

	// Log configuration for debugging
	p.logConfiguration()

	// Start metrics server
	if err := p.metricsServer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.metricsServer.Stop(shutdownCtx); err != nil {
			p.logger.Warn("Failed to stop metrics server gracefully: %v", err)
		}
	}()

	// Detect GTID mode for metrics
	if err := p.detectGTIDMode(); err != nil {
		p.logger.Warn("Failed to detect GTID mode for metrics: %v", err)
	}

	// Create CDC event channel
	eventChan := make(chan *CDCEvent, 100) // Buffered channel for events

	// Create BigQuery writer
	bqWriter, err := NewBigQueryWriter(p.cfg, p.logger, p.metrics)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery writer: %w", err)
	}
	defer bqWriter.Close()

	// Set up transaction buffer flush callback
	p.transactionBuffer.SetFlushCallback(func(events []*CDCEvent) error {
		for _, event := range events {
			if err := bqWriter.WriteEvent(ctx, event); err != nil {
				return err
			}
			// Record successful event processing
			position := event.Position.Name
			if position == "" {
				position = fmt.Sprintf("%d", event.Position.Pos)
			}
			p.metrics.RecordEvent(
				fmt.Sprintf("%s.%s", event.Database, event.Table),
				len(event.Rows),
				position,
			)
		}
		return nil
	})

	// If running in one-time mode, perform snapshot + binlog-catchup workflow
	if p.cfg.Mode == "once" {
		p.logger.Info("Starting one-time snapshot + binlog-catchup flow (mode=once)")
		if err := p.runOnce(ctx, bqWriter); err != nil {
			return fmt.Errorf("run-once failed: %w", err)
		}
		p.logger.Info("One-time sync completed successfully")
		return nil
	}

	// Create CDC reader
	cdcReader := NewCDCReader(p.cfg, p.logger)

	// Start CDC reader in a goroutine
	readerCtx, cancelReader := context.WithCancel(ctx)
	defer cancelReader()

	go func() {
		if err := cdcReader.Start(readerCtx, eventChan); err != nil {
			p.logger.CDCError("CDC reader failed", err)
		}
	}()

	// Process events from the channel
	for {
		select {
		case <-ctx.Done():
			p.logger.CDCComplete("Pipeline shutting down")
			// Flush any remaining transactions
			if err := p.transactionBuffer.FlushAll(); err != nil {
				p.logger.Warn("Failed to flush pending transactions during shutdown: %v", err)
			}
			return ctx.Err()

		case event := <-eventChan:
			// Add event to transaction buffer
			if err := p.transactionBuffer.AddEvent(event); err != nil {
				p.logger.CDCError("Failed to buffer event", err)
				p.metrics.RecordError(err)
			}
		}
	}
}

// logConfiguration logs the current configuration for debugging
func (p *Pipeline) logConfiguration() {
	p.logger.Debug("MySQL config: host=%s port=%d user=%s server_id=%d",
		p.cfg.MySQL.Host, p.cfg.MySQL.Port, p.cfg.MySQL.User, p.cfg.MySQL.ServerID)
	p.logger.Debug("BigQuery config: project=%s dataset=%s", p.cfg.BigQuery.Project, p.cfg.BigQuery.Dataset)
	p.logger.Debug("Batching config: max_rows=%d max_delay=%s", p.cfg.Batching.MaxRows, p.cfg.Batching.MaxDelay)
}

// runOnce performs a snapshot of configured tables and then catches up via binlog
// algorithm (snapshot + binlog-catchup). It stops when the binlog position/GTID
// observed after the snapshot has been processed.
func (p *Pipeline) runOnce(ctx context.Context, bqWriter *BigQueryWriter) error {
	// Create CDC reader helper (used for detecting GTID mode and getting positions)
	r := NewCDCReader(p.cfg, p.logger)

	gtidEnabled, err := r.detectGTIDMode()
	if err != nil {
		p.logger.Warn("Failed to detect GTID mode, defaulting to position-based mode: %v", err)
		gtidEnabled = false
	}

	// Capture start position/GTID BEFORE snapshot (we will stream from this point)
	var startPos *mysql.Position
	var startGTID mysql.GTIDSet

	if gtidEnabled {
		startGTID, err = r.getCurrentGTIDSet()
		if err != nil {
			p.logger.Warn("Failed to get current GTID set, falling back to position: %v", err)
			gtidEnabled = false
		}
	}

	if !gtidEnabled {
		startPos, err = r.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get binlog position for snapshot start: %w", err)
		}
	}

	p.logger.Info("Snapshot start point captured: %v", func() interface{} {
		if gtidEnabled {
			return startGTID.String()
		}
		return startPos
	}())

	// Snapshot each configured table into BigQuery (mock writer used in tests if configured)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql",
		p.cfg.MySQL.User, p.cfg.MySQL.Password, p.cfg.MySQL.Host, p.cfg.MySQL.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open MySQL connection for snapshot: %w", err)
	}
	defer db.Close()

	for _, t := range p.cfg.CDC.Tables {
		p.logger.Info("Snapshotting table %s.%s", t.DB, t.Table)

		query := fmt.Sprintf("SELECT * FROM %s.%s", t.DB, t.Table)
		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("failed to query table %s.%s: %w", t.DB, t.Table, err)
		}

		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			return fmt.Errorf("failed to read columns for %s.%s: %w", t.DB, t.Table, err)
		}

		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}

			if err := rows.Scan(valPtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan row for %s.%s: %w", t.DB, t.Table, err)
			}

			rowMap := make(map[string]interface{})
			for i, col := range cols {
				v := vals[i]
				if b, ok := v.([]byte); ok {
					rowMap[col] = string(b)
				} else {
					rowMap[col] = v
				}
			}

			event := &CDCEvent{
				Database:  t.DB,
				Table:     t.Table,
				EventType: "SNAPSHOT",
				Rows:      []map[string]interface{}{rowMap},
				Timestamp: time.Now(),
			}

			// Attach start position so insertId generation is deterministic
			if startPos != nil {
				event.Position = *startPos
			}
			if startGTID != nil {
				// store transaction placeholder; real GTID is used in checkpointing
				event.Transaction = "snapshot"
			}

			if err := bqWriter.WriteEvent(ctx, event); err != nil {
				rows.Close()
				return fmt.Errorf("failed to write snapshot row to BigQuery: %w", err)
			}
		}

		rows.Close()
		// flush per-table to ensure snapshot rows are written
		if err := bqWriter.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush snapshot data for %s.%s: %w", t.DB, t.Table, err)
		}
	}

	// Capture stop point AFTER snapshot so we know when we've caught up
	var stopPos *mysql.Position
	var stopGTID mysql.GTIDSet
	if gtidEnabled {
		stopGTID, err = r.getCurrentGTIDSet()
		if err != nil {
			return fmt.Errorf("failed to get GTID after snapshot: %w", err)
		}
	} else {
		stopPos, err = r.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get binlog position after snapshot: %w", err)
		}
	}

	p.logger.Info("Snapshot complete; will stream binlog events until %v", func() interface{} {
		if gtidEnabled {
			return stopGTID.String()
		}
		return stopPos
	}())

	// Seed checkpoint to start point so CDCReader begins from the captured start position
	store := checkpoint.NewFileStore(p.cfg.Checkpoint.Path)
	cp := &checkpoint.Checkpoint{Timestamp: time.Now().Unix()}
	if gtidEnabled {
		cp.GTIDSet = startGTID.String()
	} else {
		cp.Position = startPos
	}
	if err := store.Save(cp); err != nil {
		p.logger.Warn("Failed to save seed checkpoint: %v", err)
	}

	// Start CDC reader from seed checkpoint and apply events until stop point reached
	eventChan := make(chan *CDCEvent, 100)
	readerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cdcReader := NewCDCReader(p.cfg, p.logger)
	go func() {
		if err := cdcReader.Start(readerCtx, eventChan); err != nil {
			p.logger.CDCError("CDC reader failed", err)
		}
	}()

	// Process events until stop condition
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-eventChan:
			// write to BigQuery
			if err := bqWriter.WriteEvent(ctx, ev); err != nil {
				p.logger.CDCError("Failed to write CDC event during run-once", err)
				return err
			}

			// After writing, check checkpoint file whether we've reached stop point
			loaded, err := store.Load()
			if err == nil && loaded != nil {
				if gtidEnabled {
					if loaded.GTIDSet != "" {
						// parse and check containment
						cpSet, _ := mysql.ParseGTIDSet("mysql", loaded.GTIDSet)
						if cpSet != nil && cpSet.Contain(stopGTID) {
							p.logger.Info("Reached stop GTID set; completing one-time sync")
							_ = bqWriter.Flush(ctx)
							return nil
						}
					}
				} else {
					if loaded.Position != nil && stopPos != nil {
						// If same file, compare position. If different file name, simple string compare.
						if loaded.Position.Name == stopPos.Name {
							if loaded.Position.Pos >= stopPos.Pos {
								p.logger.Info("Reached stop binlog position; completing one-time sync")
								_ = bqWriter.Flush(ctx)
								return nil
							}
						} else {
							// best-effort: if filename lexically >= stop filename, assume caught up
							if loaded.Position.Name >= stopPos.Name {
								p.logger.Info("Reached stop binlog file; completing one-time sync")
								_ = bqWriter.Flush(ctx)
								return nil
							}
						}
					}
				}
			}
		}
	}
}

// GetMetrics returns a snapshot of current CDC metrics
func (p *Pipeline) GetMetrics() CDCMetricsSnapshot {
	return p.metrics.GetSnapshot()
}

// SetGTIDMode sets whether GTID mode is enabled for metrics tracking
func (p *Pipeline) SetGTIDMode(enabled bool) {
	p.metrics.GTIDEnabled = enabled
}

// detectGTIDMode detects if MySQL is using GTID mode and sets it on metrics
func (p *Pipeline) detectGTIDMode() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql",
		p.cfg.MySQL.User, p.cfg.MySQL.Password, p.cfg.MySQL.Host, p.cfg.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	var gtidMode string
	err = db.QueryRow("SELECT @@global.gtid_mode").Scan(&gtidMode)
	if err != nil {
		return fmt.Errorf("failed to query GTID mode: %w", err)
	}

	p.metrics.GTIDEnabled = (gtidMode == "ON")
	return nil
}

// parseLogLevel converts string log level to Level enum
func parseLogLevel(level string) logging.Level {
	switch level {
	case "debug":
		return logging.DEBUG
	case "info":
		return logging.INFO
	case "warn", "warning":
		return logging.WARN
	case "error":
		return logging.ERROR
	default:
		return logging.INFO // default to info level
	}
}
