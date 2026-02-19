package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	"github.com/ChaosHour/mysql2bq/internal/seeder/backfill"
	"github.com/spf13/cobra"
)

var backfillConfigPath string
var backfillTable string
var backfillDatabase string

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "Backfill tables from MySQL to BigQuery",
	Long: `Perform a full table backfill from MySQL to BigQuery.
This command scans the specified table(s) and streams the data to BigQuery,
recording the binlog position for seamless CDC handoff.

Examples:
  # Backfill a specific table
  mysql2bq backfill --table users --database app

  # Backfill all configured tables
  mysql2bq backfill --config config.yaml`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load(backfillConfigPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		return runBackfill(cfg)
	},
}

func init() {
	backfillCmd.Flags().StringVar(&backfillConfigPath, "config", "config.yaml", "Path to config file")
	backfillCmd.Flags().StringVar(&backfillTable, "table", "", "Specific table to backfill (optional)")
	backfillCmd.Flags().StringVar(&backfillDatabase, "database", "", "Database name (required if --table specified)")
	rootCmd.AddCommand(backfillCmd)
}

func runBackfill(cfg *config.Config) error {
	// Initialize logger
	logLevel := parseLogLevel(cfg.Logging.Level)
	logger, err := logging.NewLogger(logLevel, cfg.Logging.File)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	ctx := context.Background()
	logger.Info("Starting backfill operation")

	// Create checkpoint manager and prepare for backfill
	cpManager := backfill.NewCheckpointManager(cfg, logger)
	if err := cpManager.PrepareForBackfill(ctx); err != nil {
		return fmt.Errorf("failed to prepare for backfill: %w", err)
	}

	// Create BigQuery emitter
	emitter, err := newBigQueryEmitter(cfg)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery emitter: %w", err)
	}
	defer emitter.Close()

	// Create batch emitter for efficient processing
	batchEmitter := backfill.NewBatchEmitter(emitter, cfg.Batching.MaxRows)
	defer batchEmitter.FlushAll(ctx) // Ensure all buffered data is sent

	// Create batcher
	batcher := backfill.NewBatcher(cfg, logger, batchEmitter)

	// Determine which tables to backfill
	tables := cfg.CDC.Tables
	if backfillTable != "" {
		if backfillDatabase == "" {
			return fmt.Errorf("--database is required when --table is specified")
		}
		// Override with specific table
		tables = []struct {
			DB    string `yaml:"db"`
			Table string `yaml:"table"`
		}{
			{DB: backfillDatabase, Table: backfillTable},
		}
	}

	// Backfill each table
	startTime := time.Now()
	totalTables := len(tables)

	for i, table := range tables {
		logger.Info("Backfilling table %d/%d: %s.%s", i+1, totalTables, table.DB, table.Table)

		if err := batcher.ProcessTable(ctx, table.DB, table.Table); err != nil {
			return fmt.Errorf("failed to backfill table %s.%s: %w", table.DB, table.Table, err)
		}

		// Flush after each table to ensure data is written
		if err := batchEmitter.FlushAll(ctx); err != nil {
			return fmt.Errorf("failed to flush data for table %s.%s: %w", table.DB, table.Table, err)
		}
	}

	duration := time.Since(startTime)

	// Mark backfill as complete
	if err := cpManager.MarkBackfillComplete(); err != nil {
		logger.Warn("Failed to mark backfill complete: %v", err)
	}

	logger.Info("Backfill completed successfully: %d tables in %v", totalTables, duration)
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

// bigQueryEmitter implements RowEmitter for BigQuery
type bigQueryEmitter struct {
	emitter *cbEmitter
}

func newBigQueryEmitter(cfg *config.Config) (*bigQueryEmitter, error) {
	emitter, err := newCBEmitter(cfg.BigQuery.Project, cfg.BigQuery.Dataset)
	if err != nil {
		return nil, err
	}

	return &bigQueryEmitter{emitter: emitter}, nil
}

func (e *bigQueryEmitter) Emit(ctx context.Context, rows []map[string]interface{}, table string) error {
	return e.emitter.Emit(ctx, rows, table)
}

func (e *bigQueryEmitter) Close() error {
	return e.emitter.Close()
}
