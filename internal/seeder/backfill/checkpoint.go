package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/checkpoint"
	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// CheckpointManager handles checkpoint operations for backfill
type CheckpointManager struct {
	cfg    *config.Config
	logger *logging.Logger
	store  checkpoint.Store
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(cfg *config.Config, logger *logging.Logger) *CheckpointManager {
	return &CheckpointManager{
		cfg:    cfg,
		logger: logger,
		store:  checkpoint.NewFileStore(cfg.Checkpoint.Path),
	}
}

// PrepareForBackfill captures start position and saves checkpoint
func (c *CheckpointManager) PrepareForBackfill(ctx context.Context) error {
	recorder := NewPositionRecorder(c.cfg, c.logger)

	pos, gtidSet, err := recorder.RecordStartPosition(ctx)
	if err != nil {
		return fmt.Errorf("failed to record start position: %w", err)
	}

	if err := recorder.SaveCheckpoint(pos, gtidSet); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	return nil
}

// GetStartPosition retrieves the saved start position
func (c *CheckpointManager) GetStartPosition() (*mysql.Position, mysql.GTIDSet, error) {
	cp, err := c.store.Load()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	if cp == nil {
		return nil, nil, fmt.Errorf("no checkpoint found")
	}

	var pos *mysql.Position
	var gtidSet mysql.GTIDSet

	if cp.GTIDSet != "" {
		gtidSet, err = mysql.ParseGTIDSet("mysql", cp.GTIDSet)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse GTID set: %w", err)
		}
	} else if cp.Position != nil {
		pos = cp.Position
	}

	return pos, gtidSet, nil
}

// MarkBackfillComplete updates checkpoint to indicate backfill is done
func (c *CheckpointManager) MarkBackfillComplete() error {
	cp := &checkpoint.Checkpoint{
		Timestamp: time.Now().Unix(),
	}

	if err := c.store.Save(cp); err != nil {
		return fmt.Errorf("failed to save completion checkpoint: %w", err)
	}

	c.logger.Info("Marked backfill as completed")
	return nil
}
