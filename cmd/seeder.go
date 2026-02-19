package cmd

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/spf13/cobra"
)

var seederConfigPath string

var seederCmd = &cobra.Command{
	Use:   "seeder",
	Short: "PoC seeder using callback-based emission (Gravity-inspired)",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load(seederConfigPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		return runSeederPoC(cfg)
	},
}

func init() {
	seederCmd.Flags().StringVar(&seederConfigPath, "config", "config.yaml", "Path to config file")
	rootCmd.AddCommand(seederCmd)
}

// runSeederPoC demonstrates the callback-based emission pattern
func runSeederPoC(cfg *config.Config) error {
	ctx := context.Background()

	emitter, err := newCBEmitter(cfg.BigQuery.Project, cfg.BigQuery.Dataset)
	if err != nil {
		return fmt.Errorf("failed to create emitter: %w", err)
	}
	defer emitter.Close()

	log.Println("Starting seeder PoC with callback-based emission")

	// For each configured table, create scanner and scan
	for _, table := range cfg.CDC.Tables {
		scanner := newMockTableScanner(table.DB, table.Table, emitter)

		if err := scanner.Scan(ctx); err != nil {
			return fmt.Errorf("failed to scan table %s.%s: %w", table.DB, table.Table, err)
		}

		log.Printf("Completed scanning table %s.%s", table.DB, table.Table)
	}

	log.Println("Seeder PoC completed successfully")
	return nil
}

// cbEmitter implements a callback-based emitter for row forwarding
type cbEmitter struct {
	projectID string
	dataset   string
	table     string
	bqClient  *bigquery.Client
}

func newCBEmitter(projectID, dataset string) (*cbEmitter, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &cbEmitter{
		projectID: projectID,
		dataset:   dataset,
		bqClient:  client,
	}, nil
}

func (e *cbEmitter) Emit(ctx context.Context, rows []map[string]interface{}, table string) error {
	e.table = table

	log.Printf("Emitting %d rows to BigQuery table %s", len(rows), table)

	// Convert rows to BigQuery format
	var bigqueryRows []map[string]bigquery.Value
	for _, row := range rows {
		bqRow := make(map[string]bigquery.Value)
		for k, v := range row {
			bqRow[k] = v
		}
		bigqueryRows = append(bigqueryRows, bqRow)
	}

	// Insert into BigQuery
	inserter := e.bqClient.Dataset(e.dataset).Table(e.table).Inserter()
	return inserter.Put(ctx, bigqueryRows)
}

func (e *cbEmitter) Close() error {
	if e.bqClient != nil {
		return e.bqClient.Close()
	}
	return nil
}

// mockTableScanner simulates Gravity's mysql_table_scanner interface
type mockTableScanner struct {
	db      string
	table   string
	emitter *cbEmitter
}

func newMockTableScanner(db, table string, emitter *cbEmitter) *mockTableScanner {
	return &mockTableScanner{
		db:      db,
		table:   table,
		emitter: emitter,
	}
}

func (s *mockTableScanner) Scan(ctx context.Context) error {
	log.Printf("Scanning table %s.%s", s.db, s.table)

	// Mock data - in real implementation, this would come from MySQL
	mockRows := []map[string]interface{}{
		{"id": 1, "name": "Alice", "created_at": "2024-01-01T00:00:00Z"},
		{"id": 2, "name": "Bob", "created_at": "2024-01-02T00:00:00Z"},
	}

	// Emit rows via callback
	return s.emitter.Emit(ctx, mockRows, s.table)
}
