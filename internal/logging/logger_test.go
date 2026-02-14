package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	// Test with stdout only
	logger, err := NewLogger(INFO, "")
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	if logger.level != INFO {
		t.Errorf("Expected level INFO, got %v", logger.level)
	}
}

func TestNewLoggerWithFile(t *testing.T) {
	// Create temporary log file
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := NewLogger(DEBUG, logFile)
	if err != nil {
		t.Fatalf("Failed to create logger with file: %v", err)
	}

	// Test logging
	logger.Info("Test info message")
	logger.Error("Test error message")
	logger.Debug("Test debug message")

	// Check if file was created and contains expected content
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	contentStr := string(content)
	if !strings.Contains(contentStr, "Test info message") {
		t.Error("Log file should contain info message")
	}
	if !strings.Contains(contentStr, "Test error message") {
		t.Error("Log file should contain error message")
	}
	if !strings.Contains(contentStr, "Test debug message") {
		t.Error("Log file should contain debug message")
	}
}

func TestLoggerLevels(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := NewLogger(WARN, logFile)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// These should not be logged (below WARN level)
	logger.Info("Should not appear")
	logger.Debug("Should not appear")

	// These should be logged
	logger.Warn("Should appear")
	logger.Error("Should appear")

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	contentStr := string(content)
	if strings.Contains(contentStr, "Should not appear") {
		t.Error("Lower level messages should not appear in WARN level logs")
	}
	if !strings.Contains(contentStr, "Should appear") {
		t.Error("WARN and ERROR messages should appear")
	}
}

func TestSpecializedLogging(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := NewLogger(DEBUG, logFile)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Test specialized logging methods
	testErr := os.ErrNotExist
	logger.LogConnectionIssue("MySQL", testErr, 3)
	logger.LogDataIssue("users", "insert", testErr, 100)
	logger.LogPerformance("batch_insert", time.Second*5, 1000)
	logger.LogPipelineEvent("startup", "Pipeline initialized")
	logger.LogBigQueryOperation("insert", "users", testErr)
	logger.LogMySQLOperation("select", "users", "binlog.0001:123", nil)
	logger.LogNetworkLatency("query", time.Second*2, time.Second*1)
	logger.LogBatchOperation("users", 500, time.Millisecond*250)
	logger.LogCheckpoint("users", "binlog.0001:456")

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	contentStr := string(content)

	// Check that specialized messages are logged
	expectedMessages := []string{
		"Connection issue",
		"Data issue",
		"Performance:",
		"Pipeline event:",
		"BigQuery",
		"MySQL",
		"network latency",
		"Batch processed",
		"Checkpoint saved",
	}

	for _, msg := range expectedMessages {
		if !strings.Contains(contentStr, msg) {
			t.Errorf("Expected log message containing '%s' not found", msg)
		}
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", DEBUG},
		{"DEBUG", DEBUG},
		{"info", INFO},
		{"INFO", INFO},
		{"warn", WARN},
		{"warning", WARN},
		{"WARN", WARN},
		{"error", ERROR},
		{"ERROR", ERROR},
		{"", INFO},        // default
		{"invalid", INFO}, // default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseLogLevel(tt.input)
			if result != tt.expected {
				t.Errorf("parseLogLevel(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
