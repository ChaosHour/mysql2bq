package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
)

func TestMetricsServer_handleMetrics(t *testing.T) {
	// Create test config
	cfg := &config.Config{}
	cfg.HTTP.Enabled = true
	cfg.HTTP.Host = "localhost"
	cfg.HTTP.Port = 8080
	cfg.HTTP.Path = "/metrics"

	// Create test metrics
	metrics := NewCDCMetrics()
	metrics.RecordEvent("users", 5, "mysql-bin.000001:123")
	metrics.RecordEvent("orders", 3, "mysql-bin.000001:456")
	metrics.RecordError(fmt.Errorf("test error"))

	// Create metrics server
	logger := &logging.Logger{}
	server := NewMetricsServer(cfg, logger, metrics)

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	// Call handler
	server.handleMetrics(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}

	// Parse response
	var snapshot CDCMetricsSnapshot
	if err := json.Unmarshal(w.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}

	// Verify metrics
	if snapshot.TotalEventsProcessed != 2 {
		t.Errorf("Expected 2 total events, got %d", snapshot.TotalEventsProcessed)
	}
	if snapshot.TotalRowsProcessed != 8 {
		t.Errorf("Expected 8 total rows, got %d", snapshot.TotalRowsProcessed)
	}
	if snapshot.ErrorCount != 1 {
		t.Errorf("Expected 1 error, got %d", snapshot.ErrorCount)
	}
	if len(snapshot.TableMetrics) != 2 {
		t.Errorf("Expected 2 table metrics, got %d", len(snapshot.TableMetrics))
	}
}

func TestMetricsServer_handleHealth(t *testing.T) {
	// Create test config
	cfg := &config.Config{}
	cfg.HTTP.Enabled = true

	// Create test metrics
	metrics := NewCDCMetrics()

	// Create metrics server
	logger := &logging.Logger{}
	server := NewMetricsServer(cfg, logger, metrics)

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	// Call handler
	server.handleHealth(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}

	// Parse response
	var health map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &health); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}

	// Verify health response
	if status, ok := health["status"].(string); !ok || status != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", health["status"])
	}

	if _, ok := health["time"]; !ok {
		t.Error("Expected 'time' field in health response")
	}

	if _, ok := health["uptime"]; !ok {
		t.Error("Expected 'uptime' field in health response")
	}
}

func TestMetricsServer_handleMetrics_MethodNotAllowed(t *testing.T) {
	cfg := &config.Config{}
	cfg.HTTP.Enabled = true

	metrics := NewCDCMetrics()
	logger := &logging.Logger{}
	server := NewMetricsServer(cfg, logger, metrics)

	// Test POST request
	req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	w := httptest.NewRecorder()

	server.handleMetrics(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405 for POST, got %d", w.Code)
	}
}

func TestMetricsServer_handleHealth_MethodNotAllowed(t *testing.T) {
	cfg := &config.Config{}
	cfg.HTTP.Enabled = true

	metrics := NewCDCMetrics()
	logger := &logging.Logger{}
	server := NewMetricsServer(cfg, logger, metrics)

	// Test POST request
	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()

	server.handleHealth(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405 for POST, got %d", w.Code)
	}
}

func TestMetricsServer_StartStop(t *testing.T) {
	// Create test config with disabled server
	cfg := &config.Config{}
	cfg.HTTP.Enabled = false

	metrics := NewCDCMetrics()
	logger := &logging.Logger{}
	server := NewMetricsServer(cfg, logger, metrics)

	ctx := context.Background()

	// Start should not fail when disabled
	err := server.Start(ctx)
	if err != nil {
		t.Errorf("Start should not fail when server is disabled: %v", err)
	}

	// Stop should not fail
	err = server.Stop(ctx)
	if err != nil {
		t.Errorf("Stop should not fail: %v", err)
	}
}
