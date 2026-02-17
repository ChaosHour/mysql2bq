package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestBigQueryWriter_cdcEventToRows(t *testing.T) {
	// Create a mock BigQuery writer (without actual BigQuery client)
	writer := &BigQueryWriter{
		cfg:    nil, // Not needed for this test
		logger: nil, // Not needed for this test
		client: nil, // Mock mode
	}

	// Create a test CDC event
	event := &CDCEvent{
		Database:    "testdb",
		Table:       "users",
		EventType:   "INSERT",
		Timestamp:   time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		Position:    mysql.Position{Name: "mysql-bin.000001", Pos: 12345},
		Transaction: "abc-123",
		Rows: []map[string]interface{}{
			{
				"id":    1,
				"name":  "John Doe",
				"email": "john@example.com",
			},
			{
				"id":    2,
				"name":  "Jane Smith",
				"email": "jane@example.com",
			},
		},
	}

	// Convert to BigQuery rows
	rows := writer.cdcEventToRows(event)

	// Verify the number of rows
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	// Verify each row is a StructSaver with proper InsertID
	for i, row := range rows {
		structSaver, ok := row.(*bigquery.StructSaver)
		if !ok {
			t.Errorf("Row %d: expected *bigquery.StructSaver, got %T", i, row)
			continue
		}

		// Check InsertID format
		expectedInsertID := "(mysql-bin.000001, 12345):abc-123:0"
		if i == 1 {
			expectedInsertID = "(mysql-bin.000001, 12345):abc-123:1"
		}

		if structSaver.InsertID != expectedInsertID {
			t.Errorf("Row %d: expected InsertID '%s', got '%s'", i, expectedInsertID, structSaver.InsertID)
		}

		// Check struct data
		data, ok := structSaver.Struct.(map[string]bigquery.Value)
		if !ok {
			t.Errorf("Row %d: expected map[string]bigquery.Value, got %T", i, structSaver.Struct)
			continue
		}

		// Check CDC metadata
		if data["_cdc_timestamp"] != event.Timestamp {
			t.Errorf("Row %d: expected _cdc_timestamp %v, got %v", i, event.Timestamp, data["_cdc_timestamp"])
		}
		if data["_cdc_operation"] != "INSERT" {
			t.Errorf("Row %d: expected _cdc_operation 'INSERT', got %v", i, data["_cdc_operation"])
		}
		if data["_cdc_database"] != "testdb" {
			t.Errorf("Row %d: expected _cdc_database 'testdb', got %v", i, data["_cdc_database"])
		}
		if data["_cdc_table"] != "users" {
			t.Errorf("Row %d: expected _cdc_table 'users', got %v", i, data["_cdc_table"])
		}

		// Check row data
		if data["id"] != 1 && data["id"] != 2 {
			t.Errorf("Row %d: expected id 1 or 2, got %v", i, data["id"])
		}
		if data["name"] != "John Doe" && data["name"] != "Jane Smith" {
			t.Errorf("Row %d: unexpected name %v", i, data["name"])
		}
	}
}

func TestBigQueryWriter_cdcEventToRows_EmptyRows(t *testing.T) {
	writer := &BigQueryWriter{}

	event := &CDCEvent{
		Database:    "testdb",
		Table:       "empty_table",
		EventType:   "INSERT",
		Timestamp:   time.Now(),
		Position:    mysql.Position{Name: "mysql-bin.000001", Pos: 100},
		Transaction: "tx-1",
		Rows:        []map[string]interface{}{},
	}

	rows := writer.cdcEventToRows(event)

	if len(rows) != 0 {
		t.Errorf("Expected 0 rows for empty event, got %d", len(rows))
	}
}

func TestBigQueryWriter_Batching(t *testing.T) {
	// Create a mock config with small batch size for testing
	cfg := &config.Config{}
	cfg.BigQuery.Project = "my-project" // Mock mode
	cfg.Batching.MaxRows = 2            // Small batch size for testing

	logger := &logging.Logger{} // Mock logger
	writer, err := NewBigQueryWriter(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create BigQuery writer: %v", err)
	}

	// Create test events
	event1 := &CDCEvent{
		Database:    "testdb",
		Table:       "users",
		EventType:   "INSERT",
		Timestamp:   time.Now(),
		Position:    mysql.Position{Name: "mysql-bin.000001", Pos: 100},
		Transaction: "tx-1",
		Rows: []map[string]interface{}{
			{"id": 1, "name": "Alice"},
		},
	}

	event2 := &CDCEvent{
		Database:    "testdb",
		Table:       "users",
		EventType:   "INSERT",
		Timestamp:   time.Now(),
		Position:    mysql.Position{Name: "mysql-bin.000001", Pos: 200},
		Transaction: "tx-2",
		Rows: []map[string]interface{}{
			{"id": 2, "name": "Bob"},
		},
	}

	ctx := context.Background()

	// Write first event (should not flush yet)
	err = writer.WriteEvent(ctx, event1)
	if err != nil {
		t.Errorf("Failed to write first event: %v", err)
	}

	// Check that batch contains 1 row
	writer.batchMu.Lock()
	if writer.batchSize != 1 {
		t.Errorf("Expected batch size 1, got %d", writer.batchSize)
	}
	if len(writer.batchRows["testdb_users"]) != 1 {
		t.Errorf("Expected 1 row in testdb_users batch, got %d", len(writer.batchRows["testdb_users"]))
	}
	writer.batchMu.Unlock()

	// Write second event (should trigger flush due to batch size)
	err = writer.WriteEvent(ctx, event2)
	if err != nil {
		t.Errorf("Failed to write second event: %v", err)
	}

	// Check that batch is empty after flush
	writer.batchMu.Lock()
	if writer.batchSize != 0 {
		t.Errorf("Expected batch size 0 after flush, got %d", writer.batchSize)
	}
	if len(writer.batchRows) != 0 {
		t.Errorf("Expected empty batch map after flush, got %d tables", len(writer.batchRows))
	}
	writer.batchMu.Unlock()
}

func TestBigQueryWriter_RetryDelay(t *testing.T) {
	cfg := &config.Config{}
	cfg.BigQuery.Project = "my-project"
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.InitialDelay = "1s"
	cfg.Retry.MaxDelay = "10s"

	logger := &logging.Logger{}
	writer, err := NewBigQueryWriter(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create BigQuery writer: %v", err)
	}

	// Test retry delays
	delay0 := writer.calculateRetryDelay(0)
	if delay0 != time.Second {
		t.Errorf("Expected delay 1s for attempt 0, got %v", delay0)
	}

	delay1 := writer.calculateRetryDelay(1)
	if delay1 != 2*time.Second {
		t.Errorf("Expected delay 2s for attempt 1, got %v", delay1)
	}

	delay2 := writer.calculateRetryDelay(2)
	if delay2 != 4*time.Second {
		t.Errorf("Expected delay 4s for attempt 2, got %v", delay2)
	}
}

func TestBigQueryWriter_IsRetryableError(t *testing.T) {
	cfg := &config.Config{}
	cfg.BigQuery.Project = "my-project"

	logger := &logging.Logger{}
	writer, err := NewBigQueryWriter(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create BigQuery writer: %v", err)
	}

	testCases := []struct {
		err      error
		expected bool
	}{
		{fmt.Errorf("rate limit exceeded"), true},
		{fmt.Errorf("temporary server error"), true},
		{fmt.Errorf("service unavailable"), true},
		{fmt.Errorf("connection timeout"), true},
		{fmt.Errorf("invalid table name"), false},
		{fmt.Errorf("permission denied"), false},
	}

	for _, tc := range testCases {
		result := writer.isRetryableError(tc.err)
		if result != tc.expected {
			t.Errorf("isRetryableError(%v) = %v, expected %v", tc.err, result, tc.expected)
		}
	}
}
