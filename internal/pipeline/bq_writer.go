package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
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

	// MySQL connection for schema inspection
	mysqlDB *sql.DB

	// Metrics
	metrics *CDCMetrics

	// Batching fields - per table batches
	batchMu   sync.Mutex
	batchRows map[string][]bigquery.ValueSaver // table -> rows
	batchSize int
	lastFlush time.Time

	// Backpressure and worker pool
	writeChan   chan *writeRequest
	workerWg    sync.WaitGroup
	rateLimiter <-chan time.Time
	stopChan    chan struct{}
	running     bool
}

type writeRequest struct {
	ctx      context.Context
	database string
	table    string
	rows     []bigquery.ValueSaver
	result   chan error
}

func NewBigQueryWriter(cfg *config.Config, logger *logging.Logger, metrics *CDCMetrics) (*BigQueryWriter, error) {
	ctx := context.Background()

	// For testing without BigQuery credentials, create a mock writer
	if cfg.BigQuery.Project == "my-project" || cfg.BigQuery.Project == "" {
		// Set defaults for mock mode
		numWorkers := 3
		if cfg.BigQueryWriter.NumWorkers > 0 {
			numWorkers = cfg.BigQueryWriter.NumWorkers
		}
		channelBuffer := 100
		if cfg.BigQueryWriter.ChannelBuffer > 0 {
			channelBuffer = cfg.BigQueryWriter.ChannelBuffer
		}

		writer := &BigQueryWriter{
			cfg:       cfg,
			logger:    logger,
			metrics:   metrics,
			client:    nil, // nil client indicates mock mode
			batchRows: make(map[string][]bigquery.ValueSaver),
			lastFlush: time.Now(),
			writeChan: make(chan *writeRequest, channelBuffer),
			stopChan:  make(chan struct{}),
		}

		// Set up rate limiting if configured
		if cfg.BigQueryWriter.RateLimit > 0 {
			rate := time.Second / time.Duration(cfg.BigQueryWriter.RateLimit)
			writer.rateLimiter = time.Tick(rate)
		}

		writer.startWorkers(numWorkers)
		logger.Warn("Using mock BigQuery writer - no actual BigQuery writes will occur")
		return writer, nil
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

	// Create MySQL connection for schema inspection
	mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port)
	mysqlDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	// Test the connection
	if err := mysqlDB.Ping(); err != nil {
		mysqlDB.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	// Set defaults for production mode
	numWorkers := 5
	if cfg.BigQueryWriter.NumWorkers > 0 {
		numWorkers = cfg.BigQueryWriter.NumWorkers
	}
	channelBuffer := 100
	if cfg.BigQueryWriter.ChannelBuffer > 0 {
		channelBuffer = cfg.BigQueryWriter.ChannelBuffer
	}

	writer := &BigQueryWriter{
		cfg:       cfg,
		logger:    logger,
		metrics:   metrics,
		client:    client,
		mysqlDB:   mysqlDB,
		batchRows: make(map[string][]bigquery.ValueSaver),
		lastFlush: time.Now(),
		writeChan: make(chan *writeRequest, channelBuffer),
		stopChan:  make(chan struct{}),
	}

	// Set up rate limiting if configured
	if cfg.BigQueryWriter.RateLimit > 0 {
		rate := time.Second / time.Duration(cfg.BigQueryWriter.RateLimit)
		writer.rateLimiter = time.Tick(rate)
	}

	writer.startWorkers(numWorkers)
	return writer, nil
}

func (w *BigQueryWriter) Close() error {
	// Stop workers
	close(w.stopChan)
	w.workerWg.Wait()

	// Flush any remaining batches
	ctx := context.Background()
	if err := w.Flush(ctx); err != nil {
		w.logger.Warn("Failed to flush remaining batches during close: %v", err)
	}

	// Close MySQL connection
	if w.mysqlDB != nil {
		if err := w.mysqlDB.Close(); err != nil {
			w.logger.Warn("Failed to close MySQL connection: %v", err)
		}
	}

	if w.client != nil {
		return w.client.Close()
	}
	return nil
}

// startWorkers starts a pool of worker goroutines for processing write requests
func (w *BigQueryWriter) startWorkers(numWorkers int) {
	w.running = true
	for i := 0; i < numWorkers; i++ {
		w.workerWg.Add(1)
		go w.worker(i)
	}
}

// worker processes write requests from the channel
func (w *BigQueryWriter) worker(id int) {
	defer w.workerWg.Done()
	w.logger.Info("BigQuery writer worker %d started", id)

	for {
		select {
		case req := <-w.writeChan:
			if req == nil {
				return // channel closed
			}

			// Apply rate limiting if configured
			if w.rateLimiter != nil {
				select {
				case <-w.rateLimiter:
					// Rate limit allows this request
				case <-req.ctx.Done():
					req.result <- req.ctx.Err()
					continue
				case <-w.stopChan:
					w.logger.Info("BigQuery writer worker %d stopping", id)
					return
				}
			}

			err := w.flushTableBatchWithRetry(req.ctx, req.database, req.table, req.rows, 0)
			req.result <- err
		case <-w.stopChan:
			w.logger.Info("BigQuery writer worker %d stopping", id)
			return
		}
	}
}

// submitWriteRequest submits a write request to the worker pool with backpressure
func (w *BigQueryWriter) submitWriteRequest(ctx context.Context, database, table string, rows []bigquery.ValueSaver) error {
	req := &writeRequest{
		ctx:      ctx,
		database: database,
		table:    table,
		rows:     rows,
		result:   make(chan error, 1),
	}

	// Submit request with backpressure (blocking if channel is full)
	select {
	case w.writeChan <- req:
		// Request submitted
	case <-ctx.Done():
		return ctx.Err()
	case <-w.stopChan:
		return fmt.Errorf("writer is shutting down")
	}

	// Wait for result
	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
	if w.running {
		// Use worker pool for concurrent processing
		return w.submitWriteRequest(ctx, database, table, rows)
	} else {
		// Fallback to direct processing (for mock mode or when workers not started)
		return w.flushTableBatchWithRetry(ctx, database, table, rows, 0)
	}
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

		// Handle partial row failures (PutMultiError) specially
		if pme, ok := err.(bigquery.PutMultiError); ok {
			// Partition rows into retryable, ignored (duplicates), and permanent failures
			retryRows, ignored, permErr := splitPutMultiError(pme, rows)

			if ignored > 0 {
				w.logger.Info("Ignored %d duplicate rows for %s.%s", ignored, database, table)
			}

			if permErr != nil {
				// Permanent row-level failures — surface error after logging
				w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", database, table), permErr)
				return fmt.Errorf("permanent row insert failures: %w", permErr)
			}

			if len(retryRows) == 0 {
				// Nothing to retry (either all succeeded or were duplicates)
				w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", database, table), nil)
				w.logger.CDCProgress(fmt.Sprintf("Inserted %d rows into %s.%s (with duplicates ignored)", len(rows), database, table), len(rows))
				return nil
			}

			// Retry only the failed rows if attempts remain
			if attempt >= w.cfg.Retry.MaxAttempts {
				return fmt.Errorf("exceeded max retry attempts (%d) for table %s.%s", w.cfg.Retry.MaxAttempts, database, table)
			}

			delay := w.calculateRetryDelay(attempt)
			w.logger.Info("Retrying %d failed row(s) for %s.%s in %v (attempt %d/%d)", len(retryRows), database, table, delay, attempt+1, w.cfg.Retry.MaxAttempts)

			select {
			case <-time.After(delay):
				return w.flushTableBatchWithRetry(ctx, database, table, retryRows, attempt+1)
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Check if this is a retryable bulk error
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

		// Record write error metrics
		if w.metrics != nil {
			w.metrics.RecordBigQueryError()
		}

		return fmt.Errorf("failed to insert rows: %w", err)
	}

	w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", database, table), nil)
	w.logger.CDCProgress(fmt.Sprintf("Inserted %d rows into %s.%s", len(rows), database, table), len(rows))

	// Record successful write metrics
	if w.metrics != nil {
		w.metrics.RecordBigQueryWrite(len(rows), 0) // TODO: measure actual latency
	}

	return nil
}

func (w *BigQueryWriter) isRetryableError(err error) bool {
	// Check for common transient BigQuery errors (string fallback)
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "rate limit") ||
		strings.Contains(errStr, "temporary") ||
		strings.Contains(errStr, "unavailable") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "backenderror") ||
		strings.Contains(errStr, "internalerror") {
		return true
	}

	// Try to inspect BigQuery error reasons if available
	var pme bigquery.PutMultiError
	if ok := errors.As(err, &pme); ok {
		// If any row error appears transient, treat as retryable at batch level
		for _, rie := range pme {
			for _, ie := range rie.Errors {
				if be, ok := ie.(bigquery.Error); ok {
					switch strings.ToLower(be.Reason) {
					case "backenderror", "internalerror", "ratelimitexceeded", "serviceunavailable":
						return true
					}
				}
				// fallback: string matching
				if strings.Contains(strings.ToLower(ie.Error()), "backend") || strings.Contains(strings.ToLower(ie.Error()), "temporary") {
					return true
				}
			}
		}
	}

	return false
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

// splitPutMultiError partitions a PutMultiError into rows that should be retried,
// rows that can safely be ignored (duplicates), and permanent failures.
// Returns: retryRows, ignoredCount, permanentError (nil if none)
func splitPutMultiError(pme bigquery.PutMultiError, rows []bigquery.ValueSaver) ([]bigquery.ValueSaver, int, error) {
	var retryIdxs []int
	ignored := 0
	var permErrs []error

	for _, rie := range pme {
		// validate index boundary
		if rie.RowIndex < 0 || rie.RowIndex >= len(rows) {
			// can't map index -> row, treat as permanent
			permErrs = append(permErrs, fmt.Errorf("out-of-range row index %d", rie.RowIndex))
			continue
		}

		retryable := false
		duplicate := false

		for _, ie := range rie.Errors {
			// If bigquery.Error is available, inspect Reason
			if be, ok := ie.(bigquery.Error); ok {
				reason := strings.ToLower(be.Reason)
				msg := strings.ToLower(be.Message)
				if strings.Contains(reason, "duplicate") || strings.Contains(msg, "duplicate") {
					duplicate = true
					break
				}
				// transient/retryable reasons
				if reason == "backenderror" || reason == "internalerror" || reason == "ratelimitexceeded" || reason == "serviceunavailable" {
					retryable = true
					continue
				}
				// otherwise not retryable for this specific inner error
				continue
			}

			// Fallback: match on error string
			s := strings.ToLower(ie.Error())
			if strings.Contains(s, "duplicate") {
				duplicate = true
				break
			}
			if strings.Contains(s, "backend") || strings.Contains(s, "temporary") || strings.Contains(s, "unavailable") || strings.Contains(s, "timeout") || strings.Contains(s, "rate") {
				retryable = true
			}
		}

		if duplicate {
			ignored++
			continue
		}

		if retryable {
			retryIdxs = append(retryIdxs, rie.RowIndex)
			continue
		}

		// Permanent failure for this row
		permErrs = append(permErrs, fmt.Errorf("row %d: %v", rie.RowIndex, rie.Errors))
	}

	// assemble retry rows preserving order
	sort.Ints(retryIdxs)
	retryRows := make([]bigquery.ValueSaver, 0, len(retryIdxs))
	for _, idx := range retryIdxs {
		retryRows = append(retryRows, rows[idx])
	}

	if len(permErrs) > 0 {
		return retryRows, ignored, fmt.Errorf("permanent row insert errors: %v", permErrs)
	}

	return retryRows, ignored, nil
}

func (w *BigQueryWriter) createDataset(ctx context.Context, dataset *bigquery.Dataset) error {
	meta := &bigquery.DatasetMetadata{
		Location: w.cfg.BigQuery.DatasetLocation,
	}

	if err := dataset.Create(ctx, meta); err != nil {
		return err
	}

	return nil
}

func (w *BigQueryWriter) createTable(ctx context.Context, table *bigquery.Table, event *CDCEvent) error {
	// Try to create schema from MySQL table inspection first
	schema, err := w.inspectMySQLTableSchema(ctx, event.Database, event.Table)
	if err != nil {
		w.logger.Warn("Failed to inspect MySQL schema for %s.%s, falling back to inference: %v", event.Database, event.Table, err)
		// Fallback to inference from event data
		schema = w.inferSchemaFromEvent(event)
	}

	meta := &bigquery.TableMetadata{
		Schema: schema,
	}

	// Add time partitioning if configured
	if w.cfg.TimePartitioning != "" {
		meta.TimePartitioning = &bigquery.TimePartitioning{
			Field: w.cfg.TimePartitioning,
		}
		if w.cfg.TimePartitioningExpiration != "" {
			// Parse expiration duration
			// (implementation would go here)
		}
	}

	if err := table.Create(ctx, meta); err != nil {
		return err
	}

	w.logger.Info("Created BigQuery table: %s", table.TableID)
	return nil
}

// inspectMySQLTableSchema inspects the MySQL table schema and creates a corresponding BigQuery schema
func (w *BigQueryWriter) inspectMySQLTableSchema(ctx context.Context, database, table string) (bigquery.Schema, error) {
	query := `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := w.mysqlDB.QueryContext(ctx, query, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL schema: %w", err)
	}
	defer rows.Close()

	var schema bigquery.Schema

	// Add CDC metadata columns
	schema = append(schema, &bigquery.FieldSchema{
		Name:        "_cdc_timestamp",
		Type:        bigquery.TimestampFieldType,
		Description: "CDC event timestamp",
	})
	schema = append(schema, &bigquery.FieldSchema{
		Name:        "_cdc_operation",
		Type:        bigquery.StringFieldType,
		Description: "CDC operation type (INSERT/UPDATE/DELETE)",
	})
	schema = append(schema, &bigquery.FieldSchema{
		Name:        "_cdc_database",
		Type:        bigquery.StringFieldType,
		Description: "Source database name",
	})
	schema = append(schema, &bigquery.FieldSchema{
		Name:        "_cdc_table",
		Type:        bigquery.StringFieldType,
		Description: "Source table name",
	})

	for rows.Next() {
		var colName, dataType, isNullable string
		var colDefault *string
		var charMaxLength, numericPrecision, numericScale *int64

		err := rows.Scan(&colName, &dataType, &isNullable, &colDefault, &charMaxLength, &numericPrecision, &numericScale)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		fieldType := w.mysqlTypeToBigQueryType(dataType)
		field := &bigquery.FieldSchema{
			Name:     MySQLColumnNameToBigQuery(colName),
			Type:     fieldType,
			Required: isNullable == "NO",
		}

		schema = append(schema, field)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return schema, nil
}

// inferSchemaFromEvent creates a schema by inferring types from event data (fallback method)
func (w *BigQueryWriter) inferSchemaFromEvent(event *CDCEvent) bigquery.Schema {
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

	return schema
}

// mysqlTypeToBigQueryType converts MySQL data types to BigQuery field types
func (w *BigQueryWriter) mysqlTypeToBigQueryType(mysqlType string) bigquery.FieldType {
	// Use the existing type mapping function from types.go
	return MySQLTypeToBigQueryType(mysqlType)
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
