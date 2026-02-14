package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
)

type Pipeline struct {
	cfg    *config.Config
	logger *logging.Logger
}

func New(cfg *config.Config) (*Pipeline, error) {
	// Initialize logger
	logLevel := parseLogLevel(cfg.Logging.Level)
	logger, err := logging.NewLogger(logLevel, cfg.Logging.File)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	return &Pipeline{
		cfg:    cfg,
		logger: logger,
	}, nil
}

func (p *Pipeline) Run(ctx context.Context) error {
	p.logger.LogPipelineEvent("startup", "Pipeline initialized")
	p.logger.Info("Tables configured: %d", len(p.cfg.CDC.Tables))
	p.logger.Info("Pipeline ready - waiting for MySQL CDC events")

	// Log configuration for debugging
	p.logConfiguration()

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
				// In production, you might want to implement retry logic or dead letter queue
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

// parseLogLevel converts string log level to Level enum
func parseLogLevel(level string) logging.Level {
	switch strings.ToLower(level) {
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
