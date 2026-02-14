package pipeline

import (
	"context"
	"fmt"
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
}

func NewBigQueryWriter(cfg *config.Config, logger *logging.Logger) (*BigQueryWriter, error) {
	ctx := context.Background()

	// For testing without BigQuery credentials, create a mock writer
	if cfg.BigQuery.Project == "my-project" || cfg.BigQuery.Project == "" {
		logger.Warn("Using mock BigQuery writer - no actual BigQuery writes will occur")
		return &BigQueryWriter{
			cfg:    cfg,
			logger: logger,
			client: nil, // nil client indicates mock mode
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
		cfg:    cfg,
		logger: logger,
		client: client,
	}, nil
}

func (w *BigQueryWriter) Close() error {
	if w.client != nil {
		return w.client.Close()
	}
	return nil
}

func (w *BigQueryWriter) WriteEvent(ctx context.Context, event *CDCEvent) error {
	w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", event.Database, event.Table), nil)

	// Mock mode - just log the event
	if w.client == nil {
		w.logger.Info("[MOCK] Would write %d rows to BigQuery table %s.%s_%s",
			len(event.Rows), w.cfg.BigQuery.Project, w.cfg.BigQuery.Dataset, fmt.Sprintf("%s_%s", event.Database, event.Table))
		for i, row := range event.Rows {
			w.logger.Debug("[MOCK] Row %d: %+v", i, row)
		}
		return nil
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
	tableID := fmt.Sprintf("%s_%s", event.Database, event.Table)
	table := dataset.Table(tableID)

	// Check if table exists, create if not
	if _, err := table.Metadata(ctx); err != nil {
		if err := w.createTable(ctx, table, event); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Convert CDC event to BigQuery rows
	rows := w.cdcEventToRows(event)

	// Insert rows
	inserter := table.Inserter()
	if err := inserter.Put(ctx, rows); err != nil {
		w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", event.Database, event.Table), err)
		return fmt.Errorf("failed to insert rows: %w", err)
	}

	w.logger.LogBigQueryOperation("write", fmt.Sprintf("%s.%s", event.Database, event.Table), nil)
	w.logger.CDCProgress(fmt.Sprintf("Inserted %d rows into %s.%s", len(rows), event.Database, event.Table), len(rows))

	return nil
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

func (w *BigQueryWriter) cdcEventToRows(event *CDCEvent) []interface{} {
	rows := make([]interface{}, len(event.Rows))

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

		rows[i] = bqRow
	}

	return rows
}
