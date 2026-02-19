package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
)

// TransactionBuffer buffers CDC events by transaction to ensure atomic writes
type TransactionBuffer struct {
	cfg     *config.Config
	logger  *logging.Logger
	metrics *CDCMetrics

	// Transaction storage: transactionID -> events
	transactions map[string][]*CDCEvent
	mu           sync.RWMutex

	// Flush callback
	onFlush func([]*CDCEvent) error

	// Buffer size tracking
	bufferSize    int
	maxBufferSize int
}

// NewTransactionBuffer creates a new transaction buffer
func NewTransactionBuffer(cfg *config.Config, logger *logging.Logger, metrics *CDCMetrics) *TransactionBuffer {
	maxBufferSize := 1000 // default
	if cfg.BigQueryWriter.ChannelBuffer > 0 {
		maxBufferSize = cfg.BigQueryWriter.ChannelBuffer
	}

	return &TransactionBuffer{
		cfg:           cfg,
		logger:        logger,
		metrics:       metrics,
		transactions:  make(map[string][]*CDCEvent),
		maxBufferSize: maxBufferSize,
	}
}

// SetFlushCallback sets the callback function for flushing completed transactions
func (tb *TransactionBuffer) SetFlushCallback(callback func([]*CDCEvent) error) {
	tb.onFlush = callback
}

// AddEvent adds a CDC event to the transaction buffer
func (tb *TransactionBuffer) AddEvent(event *CDCEvent) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Handle transaction boundary events
	switch event.EventType {
	case "BEGIN":
		// Start a new transaction
		transactionID := event.Transaction
		if tb.transactions[transactionID] != nil {
			tb.logger.Warn("Transaction %s already exists, overwriting", transactionID)
		}
		tb.transactions[transactionID] = make([]*CDCEvent, 0, 10)
		tb.logger.Debug("Started transaction %s", transactionID)
		return nil
	case "COMMIT":
		// Commit the transaction
		return tb.CommitTransaction(event.Transaction)
	}

	// For data events, add to the current transaction
	transactionID := event.Transaction
	if transactionID == "" {
		// For events without transaction ID (like snapshots), treat as single-event transactions
		transactionID = fmt.Sprintf("single_%d", time.Now().UnixNano())
		event.Transaction = transactionID
	}

	// Add event to transaction
	if tb.transactions[transactionID] == nil {
		// Transaction not started yet, start it implicitly
		tb.transactions[transactionID] = make([]*CDCEvent, 0, 10)
		tb.logger.Debug("Implicitly started transaction %s", transactionID)
	}
	tb.transactions[transactionID] = append(tb.transactions[transactionID], event)
	tb.bufferSize++

	tb.logger.Debug("Added event to transaction %s (buffer size: %d)", transactionID, tb.bufferSize)

	// Update metrics
	if tb.metrics != nil {
		tb.metrics.RecordTransactionBufferUpdate(len(tb.transactions), tb.bufferSize)
	}

	// Check if we need to flush due to buffer size
	if tb.bufferSize >= tb.maxBufferSize {
		return tb.flushOldestTransaction()
	}

	return nil
}

// CommitTransaction marks a transaction as complete and flushes it
func (tb *TransactionBuffer) CommitTransaction(transactionID string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	events, exists := tb.transactions[transactionID]
	if !exists {
		tb.logger.Warn("Attempted to commit non-existent transaction: %s", transactionID)
		return nil
	}

	// Remove from buffer
	delete(tb.transactions, transactionID)
	tb.bufferSize -= len(events)

	tb.logger.Debug("Committing transaction %s with %d events", transactionID, len(events))

	// Update metrics
	if tb.metrics != nil {
		tb.metrics.TotalTransactions++
		tb.metrics.RecordTransactionBufferUpdate(len(tb.transactions), tb.bufferSize)
	}

	// Flush the transaction
	if tb.onFlush != nil {
		if err := tb.onFlush(events); err != nil {
			return fmt.Errorf("failed to flush transaction %s: %w", transactionID, err)
		}
	}

	return nil
}

// FlushAll flushes all pending transactions (called during shutdown)
func (tb *TransactionBuffer) FlushAll() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.logger.Info("Flushing all pending transactions (%d transactions, %d total events)",
		len(tb.transactions), tb.bufferSize)

	for transactionID, events := range tb.transactions {
		if tb.onFlush != nil {
			if err := tb.onFlush(events); err != nil {
				tb.logger.Error("Failed to flush transaction %s during shutdown: %v", transactionID, err)
				// Continue with other transactions
			}
		}
	}

	// Clear buffer
	tb.transactions = make(map[string][]*CDCEvent)
	tb.bufferSize = 0

	return nil
}

// flushOldestTransaction flushes the oldest transaction to prevent buffer overflow
func (tb *TransactionBuffer) flushOldestTransaction() error {
	if len(tb.transactions) == 0 {
		return nil
	}

	// Find the oldest transaction (simple implementation - could be improved with timestamps)
	var oldestTxID string
	var oldestEvents []*CDCEvent
	minLength := int(^uint(0) >> 1) // max int

	for txID, events := range tb.transactions {
		if len(events) < minLength {
			minLength = len(events)
			oldestTxID = txID
			oldestEvents = events
		}
	}

	if oldestTxID == "" {
		return nil
	}

	tb.logger.Warn("Buffer full, force-flushing oldest transaction %s with %d events", oldestTxID, len(oldestEvents))

	// Remove from buffer
	delete(tb.transactions, oldestTxID)
	tb.bufferSize -= len(oldestEvents)

	// Flush the transaction
	if tb.onFlush != nil {
		if err := tb.onFlush(oldestEvents); err != nil {
			return fmt.Errorf("failed to flush oldest transaction %s: %w", oldestTxID, err)
		}
	}

	return nil
}

// GetStats returns buffer statistics
func (tb *TransactionBuffer) GetStats() (activeTransactions int, bufferedEvents int) {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	return len(tb.transactions), tb.bufferSize
}
