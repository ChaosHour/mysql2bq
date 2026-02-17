package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/checkpoint"
	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestCDCReader_CheckpointResume(t *testing.T) {
	// Create temporary directory for checkpoint file
	tmpDir, err := os.MkdirTemp("", "cdc_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointPath := filepath.Join(tmpDir, "checkpoint.json")

	// Create test config
	cfg := &config.Config{}
	cfg.MySQL.ServerID = 1001
	cfg.MySQL.Host = "localhost"
	cfg.MySQL.Port = 3306
	cfg.MySQL.User = "root"
	cfg.MySQL.Password = "password"
	cfg.Checkpoint.Path = checkpointPath

	logger := &logging.Logger{}

	// Test 1: Create checkpoint and verify it can be loaded
	reader := NewCDCReader(cfg, logger)

	// Create a test checkpoint
	testCP := &checkpoint.Checkpoint{
		Position: &mysql.Position{
			Name: "mysql-bin.000001",
			Pos:  12345,
		},
		GTIDSet:   "",
		Timestamp: time.Now().Unix(),
	}

	// Save checkpoint
	err = reader.checkpoint.Save(testCP)
	if err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	// Load checkpoint and verify
	loadedCP, err := reader.checkpoint.Load()
	if err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	if loadedCP.Position == nil {
		t.Fatal("Loaded checkpoint position is nil")
	}

	if loadedCP.Position.Name != "mysql-bin.000001" {
		t.Errorf("Expected position name 'mysql-bin.000001', got '%s'", loadedCP.Position.Name)
	}

	if loadedCP.Position.Pos != 12345 {
		t.Errorf("Expected position 12345, got %d", loadedCP.Position.Pos)
	}
}

func TestCDCReader_ShouldCheckpoint(t *testing.T) {
	cfg := &config.Config{}
	cfg.Checkpoint.Path = "/tmp/test_checkpoint.json"
	logger := &logging.Logger{}

	reader := NewCDCReader(cfg, logger)

	// Initially should not checkpoint (events = 0, time recent)
	if reader.shouldCheckpoint() {
		t.Error("Should not checkpoint initially")
	}

	// Should checkpoint after 100 events
	reader.eventsSinceCheckpoint = 100
	if !reader.shouldCheckpoint() {
		t.Error("Should checkpoint after 100 events")
	}
	reader.eventsSinceCheckpoint = 0

	// Should checkpoint after 30 seconds
	reader.lastCheckpoint = time.Now().Add(-31 * time.Second)
	if !reader.shouldCheckpoint() {
		t.Error("Should checkpoint after 30 seconds")
	}
}

func TestCDCReader_CheckpointSaver(t *testing.T) {
	cfg := &config.Config{}
	cfg.Checkpoint.Path = "/tmp/test_checkpoint.json"
	logger := &logging.Logger{}

	reader := NewCDCReader(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start checkpoint saver
	go reader.checkpointSaver(ctx)

	// Send a checkpoint
	testCP := &checkpoint.Checkpoint{
		Position:  &mysql.Position{Name: "test-bin.000001", Pos: 999},
		Timestamp: time.Now().Unix(),
	}

	select {
	case reader.checkpointChan <- testCP:
		// Successfully sent
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send checkpoint to channel")
	}

	// Give some time for processing
	time.Sleep(50 * time.Millisecond)

	// Cancel context to stop the saver
	cancel()

	// Wait for done signal
	select {
	case <-reader.checkpointDone:
		// Successfully stopped
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Checkpoint saver did not stop properly")
	}
}

func TestCDCReader_CreateCheckpoint(t *testing.T) {
	cfg := &config.Config{}
	cfg.MySQL.ServerID = 1001
	cfg.MySQL.Host = "localhost"
	cfg.MySQL.Port = 3306
	cfg.MySQL.User = "root"
	cfg.MySQL.Password = "password"
	cfg.Checkpoint.Path = "/tmp/test_checkpoint.json"

	logger := &logging.Logger{}
	reader := NewCDCReader(cfg, logger)

	// Test position-based checkpoint creation
	// Note: This will fail to connect to MySQL, but should handle the error gracefully
	cp := reader.createCheckpoint(false)
	if cp != nil {
		t.Error("Expected nil checkpoint when MySQL connection fails")
	}

	// Test GTID-based checkpoint creation
	cp = reader.createCheckpoint(true)
	if cp != nil {
		t.Error("Expected nil checkpoint when MySQL connection fails")
	}
}
