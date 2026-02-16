package pipeline

import (
	"fmt"
	"testing"
	"time"
)

func TestCDCMetrics(t *testing.T) {
	metrics := NewCDCMetrics()

	// Test initial state
	snapshot := metrics.GetSnapshot()
	if snapshot.TotalEventsProcessed != 0 {
		t.Errorf("Expected 0 events, got %d", snapshot.TotalEventsProcessed)
	}
	if snapshot.TotalRowsProcessed != 0 {
		t.Errorf("Expected 0 rows, got %d", snapshot.TotalRowsProcessed)
	}

	// Record some events
	metrics.RecordEvent("users", 5, "mysql-bin.000001:123")
	metrics.RecordEvent("orders", 3, "mysql-bin.000001:456")
	metrics.RecordEvent("users", 2, "mysql-bin.000001:789")

	// Check metrics
	snapshot = metrics.GetSnapshot()
	if snapshot.TotalEventsProcessed != 3 {
		t.Errorf("Expected 3 events, got %d", snapshot.TotalEventsProcessed)
	}
	if snapshot.TotalRowsProcessed != 10 {
		t.Errorf("Expected 10 rows, got %d", snapshot.TotalRowsProcessed)
	}
	if snapshot.CurrentPosition != "mysql-bin.000001:789" {
		t.Errorf("Expected position 'mysql-bin.000001:789', got '%s'", snapshot.CurrentPosition)
	}

	// Check table-specific metrics
	if len(snapshot.TableMetrics) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(snapshot.TableMetrics))
	}

	usersMetrics := snapshot.TableMetrics["users"]
	if usersMetrics.EventsProcessed != 2 {
		t.Errorf("Expected 2 user events, got %d", usersMetrics.EventsProcessed)
	}
	if usersMetrics.RowsProcessed != 7 {
		t.Errorf("Expected 7 user rows, got %d", usersMetrics.RowsProcessed)
	}

	ordersMetrics := snapshot.TableMetrics["orders"]
	if ordersMetrics.EventsProcessed != 1 {
		t.Errorf("Expected 1 order event, got %d", ordersMetrics.EventsProcessed)
	}
	if ordersMetrics.RowsProcessed != 3 {
		t.Errorf("Expected 3 order rows, got %d", ordersMetrics.RowsProcessed)
	}
}

func TestCDCMetrics_ErrorRecording(t *testing.T) {
	metrics := NewCDCMetrics()

	// Record an error
	testErr := fmt.Errorf("test error")
	metrics.RecordError(testErr)

	snapshot := metrics.GetSnapshot()
	if snapshot.ErrorCount != 1 {
		t.Errorf("Expected 1 error, got %d", snapshot.ErrorCount)
	}
	if snapshot.LastError != "test error" {
		t.Errorf("Expected error message 'test error', got '%s'", snapshot.LastError)
	}
}

func TestCDCMetrics_Rates(t *testing.T) {
	metrics := NewCDCMetrics()

	// Simulate some time passing and events
	time.Sleep(100 * time.Millisecond)
	metrics.RecordEvent("test", 10, "pos:1")

	snapshot := metrics.GetSnapshot()

	// Should have some rate calculations
	if snapshot.EventsPerSecond <= 0 {
		t.Errorf("Expected positive events per second, got %f", snapshot.EventsPerSecond)
	}
	if snapshot.RowsPerSecond <= 0 {
		t.Errorf("Expected positive rows per second, got %f", snapshot.RowsPerSecond)
	}
	if snapshot.Uptime <= 0 {
		t.Errorf("Expected positive uptime, got %v", snapshot.Uptime)
	}
}
