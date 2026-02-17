package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	"google.golang.org/api/option"
)

type BigQueryWriter struct {
	cfg    *config.Config
	logger *logging.Logger
	client *bigquery.Client

	// Batching fields - per table batches
	batchMu   sync.Mutex
	batchRows map[string][]bigquery.ValueSaver // table -> rows
	batchSize int
	lastFlush time.Time
}

func NewBigQueryWriter(cfg *config.Config, logger *logging.Logger) (*BigQueryWriter, error) {
	ctx := context.Background()

	// For testing without BigQuery credentials, create a mock writer
	if cfg.BigQuery.Project == "my-project" || cfg.BigQuery.Project == "" {
		logger.Warn("Using mock BigQuery writer - no actual BigQuery writes will occur")
		return &BigQueryWriter{
			cfg:       cfg,
			logger:    logger,
			client:    nil, // nil client indicates mock mode
			batchRows: make(map[string][]bigquery.ValueSaver),
			lastFlush: time.Now(),
		}, nil
	}

	var clientOpts []option.ClientOption

	// Configure authentication
	if cfg.BigQuery.ServiceAccountKeyJSON != "" {
		// Use service account key if provided
		clientOpts = append(clientOpts, option.WithCredentialsJSON([]byte(cfg.BigQuery.ServiceAccountKeyJSON)))
	}
	// If no service account key, use Application Default Credentials (ADC)
	// which will check GOOGLE_APPLICATION_CREDENTIALS env var

	// Create BigQuery client
	client, err := bigquery.NewClient(ctx, cfg.BigQuery.Project, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &BigQueryWriter{
		cfg:       cfg,
		logger:    logger,
		client:    client,
		batchRows: make(map[string][]bigquery.ValueSaver),
		lastFlush: time.Now(),
	}, nil
}

func (w *BigQueryWriter) Close() error {
	// Flush any remaining batches
	ctx := context.Background()
	if err := w.Flush(ctx); err != nil {
		w.logger.Warn("Failed to flush remaining batches during close: %v", err)
	}

	if w.client != nil {
		return w.client.Close()
	}
	return nil
}

func (w *BigQueryWriter) WriteEvent(ctx context.Context, event *CDCEvent) error {
	w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", event.Database, event.Table), nil)

	// Convert CDC event to BigQuery rows
	rows := w.cdcEventToRows(event)
	tableKey := fmt.Sprintf("%s_%s", event.Database, event.Table)

	// Add rows to batch
	w.batchMu.Lock()
	if w.batchRows[tableKey] == nil {
		w.batchRows[tableKey] = make([]bigquery.ValueSaver, 0, w.cfg.Batching.MaxRows)
	}
	w.batchRows[tableKey] = append(w.batchRows[tableKey], rows...)
	w.batchSize += len(rows)
	shouldFlush := w.batchSize >= w.cfg.Batching.MaxRows
	w.batchMu.Unlock()

	// Mock mode - just log the event
	if w.client == nil {
		w.logger.Info("[MOCK] Would write %d rows to BigQuery table %s.%s_%s",
			len(event.Rows), w.cfg.BigQuery.Project, w.cfg.BigQuery.Dataset, fmt.Sprintf("%s_%s", event.Database, event.Table))
		for i, row := range event.Rows {
			w.logger.Debug("[MOCK] Row %d: %+v", i, row)
		}

		// In mock mode, flush immediately for testing
		if shouldFlush {
			return w.Flush(ctx)
		}
		return nil
	}

	// Flush if batch is full
	if shouldFlush {
		return w.Flush(ctx)
	}

	return nil
}

func (w *BigQueryWriter) Flush(ctx context.Context) error {
	w.batchMu.Lock()
	batches := w.batchRows
	w.batchRows = make(map[string][]bigquery.ValueSaver)
	w.batchSize = 0
	w.lastFlush = time.Now()
	w.batchMu.Unlock()

	// If no batches to flush, return early
	if len(batches) == 0 {
		return nil
	}

	// Mock mode - just log the flush
	if w.client == nil {
		totalRows := 0
		for _, rows := range batches {
			totalRows += len(rows)
		}
		w.logger.Info("[MOCK] Would flush %d rows across %d tables", totalRows, len(batches))
		return nil
	}

	totalRows := 0
	for _, rows := range batches {
		totalRows += len(rows)
	}
	w.logger.Info("Flushing %d rows across %d tables", totalRows, len(batches))

	// Process each table batch
	for tableKey, rows := range batches {
		if len(rows) == 0 {
			continue
		}

		// Parse table key back to database and table
		parts := strings.Split(tableKey, "_")
		if len(parts) != 2 {
			w.logger.Warn("Invalid table key format: %s", tableKey)
			continue
		}
		database, table := parts[0], parts[1]

		if err := w.flushTableBatch(ctx, database, table, rows); err != nil {
			return fmt.Errorf("failed to flush batch for table %s.%s: %w", database, table, err)
		}
	}

	return nil
}

func (w *BigQueryWriter) flushTableBatch(ctx context.Context, database, table string, rows []bigquery.ValueSaver) error {
	return w.flushTableBatchWithRetry(ctx, database, table, rows, 0)
}

func (w *BigQueryWriter) flushTableBatchWithRetry(ctx context.Context, database, table string, rows []bigquery.ValueSaver, attempt int) error {
	// Check if we've exceeded max attempts
	if attempt >= w.cfg.Retry.MaxAttempts {
		return fmt.Errorf("exceeded max retry attempts (%d) for table %s.%s", w.cfg.Retry.MaxAttempts, database, table)
	}

	// Get or create dataset
	dataset := w.client.Dataset(w.cfg.BigQuery.Dataset)
	if _, err := dataset.Metadata(ctx); err != nil {
		// Dataset doesn't exist, create it
		if err := w.createDataset(ctx, dataset); err != nil {
			return fmt.Errorf("failed to create dataset: %w", err)
		}
	}

	// Get or create table
	tableID := fmt.Sprintf("%s_%s", database, table)
	bqTable := dataset.Table(tableID)

	// Check if table exists, create if not
	if _, err := bqTable.Metadata(ctx); err != nil {
		if err := w.createTable(ctx, bqTable, &CDCEvent{Database: database, Table: table}); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Insert rows
	inserter := bqTable.Inserter()
	if err := inserter.Put(ctx, rows); err != nil {
		w.logger.Warn("Failed to insert batch for table %s.%s (attempt %d/%d): %v",
			database, table, attempt+1, w.cfg.Retry.MaxAttempts, err)

		// Check if this is a retryable error
		if w.isRetryableError(err) {
			// Calculate delay with exponential backoff
			delay := w.calculateRetryDelay(attempt)
			w.logger.Info("Retrying table %s.%s flush in %v", database, table, delay)

			select {
			case <-time.After(delay):
				return w.flushTableBatchWithRetry(ctx, database, table, rows, attempt+1)
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Non-retryable error
		w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", database, table), err)
		return fmt.Errorf("failed to insert rows: %w", err)
	}

	w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", database, table), nil)
	w.logger.CDCProgress(fmt.Sprintf("Inserted %d rows into %s.%s", len(rows), database, table), len(rows))

	return nil
}

func (w *BigQueryWriter) isRetryableError(err error) bool {
	// Check for common transient BigQuery errors
	errStr := err.Error()

	// Rate limiting, temporary server errors, network issues
	return strings.Contains(errStr, "rate limit") ||
		strings.Contains(errStr, "temporary") ||
		strings.Contains(errStr, "unavailable") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection")
}

func (w *BigQueryWriter) calculateRetryDelay(attempt int) time.Duration {
	// Parse initial delay
	initialDelay, err := time.ParseDuration(w.cfg.Retry.InitialDelay)
	if err != nil {
		initialDelay = time.Second
	}

	// Parse max delay
	maxDelay, err := time.ParseDuration(w.cfg.Retry.MaxDelay)
	if err != nil {
		maxDelay = 30 * time.Second
	}

	// Exponential backoff: initialDelay * 2^attempt
	delay := initialDelay * time.Duration(1<<uint(attempt))

	// Cap at max delay
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

func (w *BigQueryWriter) createDataset(ctx context.Context, dataset *bigquery.Dataset) error {
	meta := &bigquery.DatasetMetadata{
		Location: w.cfg.BigQuery.DatasetLocation,
	}

	if err := dataset.Create(ctx, meta); err != nil {
		return err
	}

	w.logger.Info("Created BigQuery dataset: %s", w.cfg.BigQuery.Dataset)
	return nil
}

func (w *BigQueryWriter) createTable(ctx context.Context, table *bigquery.Table, event *CDCEvent) error {
	// Create schema based on the event data
	// For now, create a simple schema - in production this would be more sophisticated
	schema := bigquery.Schema{
		{Name: "_cdc_timestamp", Type: bigquery.TimestampFieldType},
		{Name: "_cdc_operation", Type: bigquery.StringFieldType},
		{Name: "_cdc_database", Type: bigquery.StringFieldType},
		{Name: "_cdc_table", Type: bigquery.StringFieldType},
	}

	// Add columns for each field in the first row
	if len(event.Rows) > 0 {
		for key, value := range event.Rows[0] {
			fieldType := w.inferBigQueryType(value)
			schema = append(schema, &bigquery.FieldSchema{
				Name: key,
				Type: fieldType,
			})
		}
	}

	meta := &bigquery.TableMetadata{
		Schema: schema,
	}

	if err := table.Create(ctx, meta); err != nil {
		return err
	}

	w.logger.Info("Created BigQuery table: %s", table.TableID)
	return nil
}

func (w *BigQueryWriter) inferBigQueryType(value interface{}) bigquery.FieldType {
	if value == nil {
		return bigquery.StringFieldType
	}

	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return bigquery.IntegerFieldType
	case float32, float64:
		return bigquery.FloatFieldType
	case bool:
		return bigquery.BooleanFieldType
	case string:
		return bigquery.StringFieldType
	case time.Time:
		return bigquery.TimestampFieldType
	case []byte:
		return bigquery.BytesFieldType
	default:
		return bigquery.StringFieldType
	}
}

func (w *BigQueryWriter) cdcEventToRows(event *CDCEvent) []bigquery.ValueSaver {
	rows := make([]bigquery.ValueSaver, len(event.Rows))

	for i, row := range event.Rows {
		// Create a new map with CDC metadata + row data
		bqRow := make(map[string]bigquery.Value)

		// Add CDC metadata
		bqRow["_cdc_timestamp"] = event.Timestamp
		bqRow["_cdc_operation"] = event.EventType
		bqRow["_cdc_database"] = event.Database
		bqRow["_cdc_table"] = event.Table

		// Add row data
		for key, value := range row {
			bqRow[key] = value
		}

		// Generate insertId for idempotency
		// Use combination of position, transaction, and row index for uniqueness
		insertId := fmt.Sprintf("%s:%s:%d", event.Position.String(), event.Transaction, i)

		rows[i] = &bigquery.StructSaver{
			Struct:   bqRow,
			InsertID: insertId,
		}
	}

	return rows
}
