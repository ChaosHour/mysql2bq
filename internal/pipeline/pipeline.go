package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	_ "github.com/go-sql-driver/mysql"
)

type Pipeline struct {
	cfg           *config.Config
	logger        *logging.Logger
	metrics       *CDCMetrics
	metricsServer *MetricsServer
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
		cfg:           cfg,
		logger:        logger,
		metrics:       metrics,
		metricsServer: NewMetricsServer(cfg, logger, metrics),
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
	bqWriter, err := NewBigQueryWriter(p.cfg, p.logger)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery writer: %w", err)
	}
	defer bqWriter.Close()

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
			return ctx.Err()

		case event := <-eventChan:
			// Write event to BigQuery
			if err := bqWriter.WriteEvent(ctx, event); err != nil {
				p.logger.CDCError("Failed to write event to BigQuery", err)
				p.metrics.RecordError(err)
				// In production, you might want to implement retry logic or dead letter queue
			} else {
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
