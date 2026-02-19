package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ChaosHour/mysql2bq/internal/config"
)

// cbEmitter implements a callback-based emitter for row forwarding
type cbEmitter struct {
	bqClient *bigquery.Client
	dataset  string
	table    string
}

func newCBEmitter(projectID, dataset string) (*cbEmitter, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &cbEmitter{
		bqClient: client,
		dataset:  dataset,
		table:    "", // Will be set per table
	}, nil
}

func (e *cbEmitter) Emit(ctx context.Context, rows []map[string]interface{}, table string) error {
	e.table = table

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
	return e.bqClient.Close()
}

// mockTableScanner simulates Gravity's mysql_table_scanner interface
// In real implementation, this would wrap Gravity's scanner
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
	// Simulate scanning a table
	// In real Gravity integration, this would use:
	// github.com/moiot/gravity/pkg/inputs/mysqlbatch/mysql_table_scanner

	log.Printf("Scanning table %s.%s", s.db, s.table)

	// Mock data - in real implementation, this would come from MySQL
	mockRows := []map[string]interface{}{
		{"id": 1, "name": "Alice", "created_at": time.Now()},
		{"id": 2, "name": "Bob", "created_at": time.Now()},
	}

	// Emit rows via callback
	return s.emitter.Emit(ctx, mockRows, s.table)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: seeder-poc <config-file>")
	}

	configPath := os.Args[1]

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx := context.Background()

	// Create BigQuery emitter
	emitter, err := newCBEmitter(cfg.BigQuery.Project, cfg.BigQuery.Dataset)
	if err != nil {
		log.Fatalf("Failed to create emitter: %v", err)
	}
	defer emitter.Close()

	log.Println("Starting seeder PoC with callback-based emission")

	// For each configured table, create scanner and scan
	for _, table := range cfg.CDC.Tables {
		scanner := newMockTableScanner(table.DB, table.Table, emitter)

		if err := scanner.Scan(ctx); err != nil {
			log.Fatalf("Failed to scan table %s.%s: %v", table.DB, table.Table, err)
		}

		log.Printf("Completed scanning table %s.%s", table.DB, table.Table)
	}

	log.Println("Seeder PoC completed successfully")
}
