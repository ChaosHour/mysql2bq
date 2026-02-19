package pipeline

import (
	"sync"
	"time"
)

// CDCMetrics tracks CDC processing metrics
type CDCMetrics struct {
	mu sync.RWMutex

	// Event metrics
	TotalEventsProcessed int64
	EventsPerSecond      float64
	LastEventTime        time.Time

	// Row metrics
	TotalRowsProcessed int64
	RowsPerSecond      float64
	LastRowTime        time.Time

	// Position tracking
	CurrentPosition string // GTID set or binlog position
	GTIDEnabled     bool

	// Transaction metrics
	ActiveTransactions    int
	TotalTransactions     int64
	TransactionBufferSize int

	// BigQuery write metrics
	TotalBatchesWritten int64
	TotalWriteErrors    int64
	AverageBatchSize    float64
	WriteLatencyMs      float64

	// Backpressure metrics
	ChannelUtilization float64
	WorkerUtilization  float64

	// Table-specific metrics
	TableMetrics map[string]*TableMetrics

	// Error tracking
	ErrorCount    int64
	LastError     string
	LastErrorTime time.Time

	// Timing
	StartTime time.Time
	Uptime    time.Duration
}

// TableMetrics tracks metrics for individual tables
type TableMetrics struct {
	TableName         string
	EventsProcessed   int64
	RowsProcessed     int64
	LastProcessedTime time.Time
	CurrentPosition   string
}

// NewCDCMetrics creates a new metrics tracker
func NewCDCMetrics() *CDCMetrics {
	return &CDCMetrics{
		TableMetrics: make(map[string]*TableMetrics),
		StartTime:    time.Now(),
	}
}

// RecordEvent records processing of a CDC event
func (m *CDCMetrics) RecordEvent(table string, rowCount int, position string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// Update global metrics
	m.TotalEventsProcessed++
	m.TotalRowsProcessed += int64(rowCount)
	m.LastEventTime = now
	m.LastRowTime = now
	m.CurrentPosition = position

	// Calculate rates (simple moving average over last minute)
	elapsed := now.Sub(m.StartTime).Seconds()
	if elapsed > 0 {
		m.EventsPerSecond = float64(m.TotalEventsProcessed) / elapsed
		m.RowsPerSecond = float64(m.TotalRowsProcessed) / elapsed
	}

	// Update table-specific metrics
	if m.TableMetrics[table] == nil {
		m.TableMetrics[table] = &TableMetrics{
			TableName: table,
		}
	}
	tableMetric := m.TableMetrics[table]
	tableMetric.EventsProcessed++
	tableMetric.RowsProcessed += int64(rowCount)
	tableMetric.LastProcessedTime = now
	tableMetric.CurrentPosition = position

	m.Uptime = now.Sub(m.StartTime)
}

// RecordTransactionBufferUpdate updates transaction buffer metrics
func (m *CDCMetrics) RecordTransactionBufferUpdate(activeTransactions int, bufferSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ActiveTransactions = activeTransactions
	m.TransactionBufferSize = bufferSize
}

// RecordBigQueryWrite records a successful BigQuery batch write
func (m *CDCMetrics) RecordBigQueryWrite(batchSize int, latencyMs float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalBatchesWritten++
	m.TotalRowsProcessed += int64(batchSize)

	// Update average batch size
	if m.TotalBatchesWritten == 1 {
		m.AverageBatchSize = float64(batchSize)
	} else {
		m.AverageBatchSize = (m.AverageBatchSize*float64(m.TotalBatchesWritten-1) + float64(batchSize)) / float64(m.TotalBatchesWritten)
	}

	// Update write latency (exponential moving average)
	if m.WriteLatencyMs == 0 {
		m.WriteLatencyMs = latencyMs
	} else {
		m.WriteLatencyMs = 0.1*latencyMs + 0.9*m.WriteLatencyMs // EMA with alpha=0.1
	}
}

// RecordBigQueryError records a BigQuery write error
func (m *CDCMetrics) RecordBigQueryError() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalWriteErrors++
}

// RecordError records an error
func (m *CDCMetrics) RecordError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ErrorCount++
	m.LastError = err.Error()
	m.LastErrorTime = time.Now()
}

// RecordBackpressure records backpressure metrics
func (m *CDCMetrics) RecordBackpressure(channelUtilization, workerUtilization float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ChannelUtilization = channelUtilization
	m.WorkerUtilization = workerUtilization
}

// GetSnapshot returns a thread-safe snapshot of current metrics
func (m *CDCMetrics) GetSnapshot() CDCMetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := CDCMetricsSnapshot{
		TotalEventsProcessed:  m.TotalEventsProcessed,
		EventsPerSecond:       m.EventsPerSecond,
		LastEventTime:         m.LastEventTime,
		TotalRowsProcessed:    m.TotalRowsProcessed,
		RowsPerSecond:         m.RowsPerSecond,
		LastRowTime:           m.LastRowTime,
		CurrentPosition:       m.CurrentPosition,
		GTIDEnabled:           m.GTIDEnabled,
		ActiveTransactions:    m.ActiveTransactions,
		TotalTransactions:     m.TotalTransactions,
		TransactionBufferSize: m.TransactionBufferSize,
		TotalBatchesWritten:   m.TotalBatchesWritten,
		TotalWriteErrors:      m.TotalWriteErrors,
		AverageBatchSize:      m.AverageBatchSize,
		WriteLatencyMs:        m.WriteLatencyMs,
		ChannelUtilization:    m.ChannelUtilization,
		WorkerUtilization:     m.WorkerUtilization,
		ErrorCount:            m.ErrorCount,
		LastError:             m.LastError,
		LastErrorTime:         m.LastErrorTime,
		StartTime:             m.StartTime,
		Uptime:                m.Uptime,
		TableMetrics:          make(map[string]TableMetricsSnapshot),
	}

	for table, metrics := range m.TableMetrics {
		snapshot.TableMetrics[table] = TableMetricsSnapshot{
			TableName:         metrics.TableName,
			EventsProcessed:   metrics.EventsProcessed,
			RowsProcessed:     metrics.RowsProcessed,
			LastProcessedTime: metrics.LastProcessedTime,
			CurrentPosition:   metrics.CurrentPosition,
		}
	}

	return snapshot
}

// CDCMetricsSnapshot is a thread-safe snapshot of metrics
type CDCMetricsSnapshot struct {
	TotalEventsProcessed  int64                           `json:"total_events_processed"`
	EventsPerSecond       float64                         `json:"events_per_second"`
	LastEventTime         time.Time                       `json:"last_event_time"`
	TotalRowsProcessed    int64                           `json:"total_rows_processed"`
	RowsPerSecond         float64                         `json:"rows_per_second"`
	LastRowTime           time.Time                       `json:"last_row_time"`
	CurrentPosition       string                          `json:"current_position"`
	GTIDEnabled           bool                            `json:"gtid_enabled"`
	ActiveTransactions    int                             `json:"active_transactions"`
	TotalTransactions     int64                           `json:"total_transactions"`
	TransactionBufferSize int                             `json:"transaction_buffer_size"`
	TotalBatchesWritten   int64                           `json:"total_batches_written"`
	TotalWriteErrors      int64                           `json:"total_write_errors"`
	AverageBatchSize      float64                         `json:"average_batch_size"`
	WriteLatencyMs        float64                         `json:"write_latency_ms"`
	ChannelUtilization    float64                         `json:"channel_utilization"`
	WorkerUtilization     float64                         `json:"worker_utilization"`
	ErrorCount            int64                           `json:"error_count"`
	LastError             string                          `json:"last_error,omitempty"`
	LastErrorTime         time.Time                       `json:"last_error_time,omitempty"`
	StartTime             time.Time                       `json:"start_time"`
	Uptime                time.Duration                   `json:"uptime"`
	TableMetrics          map[string]TableMetricsSnapshot `json:"table_metrics"`
}

// TableMetricsSnapshot is a snapshot of table-specific metrics
type TableMetricsSnapshot struct {
	TableName         string    `json:"table_name"`
	EventsProcessed   int64     `json:"events_processed"`
	RowsProcessed     int64     `json:"rows_processed"`
	LastProcessedTime time.Time `json:"last_processed_time"`
	CurrentPosition   string    `json:"current_position"`
}
