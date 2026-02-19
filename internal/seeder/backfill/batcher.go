package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
)

// Batcher handles batching of rows for efficient processing
type Batcher struct {
	cfg     *config.Config
	logger  *logging.Logger
	emitter RowEmitter
}

// NewBatcher creates a new batcher
func NewBatcher(cfg *config.Config, logger *logging.Logger, emitter RowEmitter) *Batcher {
	return &Batcher{
		cfg:     cfg,
		logger:  logger,
		emitter: emitter,
	}
}

// ProcessTable performs batched backfill of a table
func (b *Batcher) ProcessTable(ctx context.Context, dbName, tableName string) error {
	scanner := NewTableScanner(b.cfg, b.logger)

	b.logger.Info("Starting batched backfill for %s.%s", dbName, tableName)

	startTime := time.Now()
	err := scanner.ScanTable(ctx, dbName, tableName, b.emitter)
	duration := time.Since(startTime)

	if err != nil {
		return fmt.Errorf("failed to scan table %s.%s: %w", dbName, tableName, err)
	}

	b.logger.Info("Completed batched backfill for %s.%s in %v", dbName, tableName, duration)
	return nil
}

// BatchEmitter wraps an emitter with batching logic
type BatchEmitter struct {
	emitter   RowEmitter
	batchSize int
	buffer    map[string][]map[string]interface{} // table -> rows
}

// NewBatchEmitter creates a new batch emitter
func NewBatchEmitter(emitter RowEmitter, batchSize int) *BatchEmitter {
	return &BatchEmitter{
		emitter:   emitter,
		batchSize: batchSize,
		buffer:    make(map[string][]map[string]interface{}),
	}
}

// Emit adds rows to the batch buffer and flushes when batch size is reached
func (b *BatchEmitter) Emit(ctx context.Context, rows []map[string]interface{}, table string) error {
	if b.buffer[table] == nil {
		b.buffer[table] = make([]map[string]interface{}, 0, b.batchSize)
	}

	b.buffer[table] = append(b.buffer[table], rows...)

	// Flush if batch size exceeded
	if len(b.buffer[table]) >= b.batchSize {
		return b.Flush(ctx, table)
	}

	return nil
}

// Flush sends all buffered rows for a table
func (b *BatchEmitter) Flush(ctx context.Context, table string) error {
	if len(b.buffer[table]) == 0 {
		return nil
	}

	err := b.emitter.Emit(ctx, b.buffer[table], table)
	if err != nil {
		return err
	}

	// Clear buffer
	b.buffer[table] = b.buffer[table][:0]
	return nil
}

// FlushAll sends all buffered rows for all tables
func (b *BatchEmitter) FlushAll(ctx context.Context) error {
	for table := range b.buffer {
		if err := b.Flush(ctx, table); err != nil {
			return err
		}
	}
	return nil
}
