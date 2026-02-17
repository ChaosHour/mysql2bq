# mysql2bq

> ⚠️ **WARNING: This is a Proof of Concept (POC) and not production-ready code. Use at your own risk.**

A tool for Change Data Capture (CDC) from MySQL to BigQuery.

## Overview

mysql2bq replicates data changes from MySQL databases to Google BigQuery in real-time using CDC techniques.

## Features

- Real-time data replication from MySQL to BigQuery
- **Automatic GTID or position-based replication detection**
- **Comprehensive metrics tracking for monitoring CDC progress**
- **Idempotent inserts to BigQuery using insertId for data consistency**
- **Batch processing with configurable size limits for efficient data transfer**
- **Automatic retry logic with exponential backoff for resilient writes**
- **HTTP metrics endpoint for real-time monitoring**
- **Asynchronous checkpointing for reliable binlog resume**
- Configurable table selection
- Batching for efficient data transfer
- Checkpointing for resumable operations (supports both GTID and position checkpoints)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/ChaosHour/mysql2bq.git
   cd mysql2bq
   ```

2. Install dependencies:

   ```bash
   go mod tidy
   ```

3. Build the binary:

   ```bash
   make build
   ```

## Docker Setup

For development and testing, you can use Docker and Docker Compose to run the application with a MySQL database.

### Quick Start with Docker Compose

1. Ensure Docker and Docker Compose are installed

2. Start the services:

   ```bash
   docker-compose up -d
   ```

   This will start:
   - MySQL 8.0 with CDC enabled and sample data
   - The mysql2bq application using `config.docker.yaml`

3. Check logs:

   ```bash
   docker-compose logs -f mysql2bq
   ```

4. Stop the services:

   ```bash
   docker-compose down
   ```

### Using Make targets

The Makefile includes convenient Docker targets:

```bash
make docker-compose-up      # Start services
make docker-compose-down    # Stop services
make docker-compose-logs    # View logs
make docker-build          # Build Docker image
```

### Development Workflow

```bash
# Initial setup
make docker-compose-up

# After code changes
make docker-build
make docker-compose-up  # or just docker-compose restart mysql2bq

# Quick restart (if only config changed)
docker-compose restart mysql2bq

# View logs
make docker-compose-logs
```

### Docker Commands

Build the Docker image:

```bash
make docker-build
```

Run with Docker (requires MySQL running separately):

```bash
make docker-run
```

### Logging in Docker

By default, the Docker configuration (`config.docker.yaml`) logs only to stdout/stderr for better container logging. To enable file logging, uncomment the `file` line in the logging section and ensure proper volume mounting for log persistence.

## Configuration

Create a `config.yaml` file based on the provided example. For Docker development, use `config.docker.yaml` which is pre-configured to work with the Docker Compose MySQL service.

Key settings include:

- MySQL connection details
- BigQuery project and dataset
- Tables to replicate
- Batching parameters
- Checkpoint configuration

## GTID and Position-Based Replication

mysql2bq automatically detects whether your MySQL server is using GTID (Global Transaction Identifier) mode or position-based replication:

- **GTID Mode**: If `@@global.gtid_mode = 'ON'`, the tool will use GTID-based synchronization for more reliable replication across server restarts and topology changes.
- **Position-Based Mode**: If GTID is not enabled, it falls back to traditional binlog file position-based replication.

The tool will log which mode it's using at startup:

```text
MySQL GTID mode detected, using GTID-based replication
```

or

```text
Using position-based replication
```

Checkpoints are stored appropriately for each mode:

- GTID mode: Stores the executed GTID set
- Position mode: Stores binlog file name and position

## Metrics and Monitoring

mysql2bq tracks comprehensive metrics for monitoring CDC progress:

### Global Metrics

- **Total Events Processed**: Number of CDC events (INSERT/UPDATE/DELETE) processed
- **Total Rows Processed**: Total number of data rows replicated
- **Events/Rows Per Second**: Current processing rates
- **Current Position**: Latest GTID set or binlog position
- **Uptime**: How long the pipeline has been running
- **Error Count**: Number of processing errors encountered

### Table-Specific Metrics

- **Events Processed**: Per-table event counts
- **Rows Processed**: Per-table row counts
- **Last Processed Time**: When each table was last updated
- **Current Position**: Latest position for each table

### Accessing Metrics

Metrics are tracked internally and can be accessed programmatically for monitoring dashboards or alerting systems.

## BigQuery Idempotency

mysql2bq ensures data consistency by using BigQuery's `insertId` feature for idempotent inserts. Each row inserted into BigQuery includes a unique insertId generated from:

- Binlog position (file name and position)
- Transaction identifier
- Row index within the event

This prevents duplicate rows from being inserted if the same CDC event is processed multiple times, which can happen during:

- Application restarts
- Network interruptions
- BigQuery transient errors

The insertId format is: `(binlog-file, position):transaction:row-index`

Important caveat — BigQuery deduplication window

- BigQuery performs best-effort de-duplication for `insertId` values and the deduplication window is short (approximately 1 minute).
- If the same row is retried after the deduplication window has passed (for example, long outages or delayed retries), duplicates may still appear in the target table.
- To guard against this, combine `insertId` with an idempotent downstream schema (unique key), shorter checkpoint intervals, or application-level deduplication as needed.

GTID caveats and resume behavior

- GTID-based resume is preferred, but GTIDs can be purged on source servers (e.g. `@@global.gtid_purged`), which may make an older checkpoint impossible to resume by GTID alone.
- mysql2bq attempts a GTID resume when possible — if GTID resume fails (for example due to purged GTIDs), it will automatically fall back to position-based start and log a warning.
- Always verify GTID/backup procedures when relying on GTID resumes in production.

## Batch Processing and Retry Logic

mysql2bq optimizes data transfer efficiency and reliability through configurable batching and automatic retry mechanisms:

### Batch Processing

- **Configurable Batch Size**: Rows are accumulated in memory until reaching `batching.max_rows`
- **Per-Table Batching**: Separate batches are maintained for each target BigQuery table
- **Automatic Flushing**: Batches are flushed when they reach the configured size limit

### Retry Logic

- **Exponential Backoff**: Failed inserts are retried with increasing delays
- **Configurable Attempts**: Maximum retry attempts can be set via `retry.max_attempts` (default: 3)
- **Smart Error Detection**: Only transient errors (rate limits, temporary server issues, network problems) are retried
- **Partial-row handling**: On partial InsertAll failures, mysql2bq inspects per-row errors, automatically retries only the retryable rows, and ignores duplicate-insertId errors (treated as successful) to avoid unnecessary failures
- **Graceful Degradation**: Non-retryable errors fail immediately to prevent data loss

### Configurations for batching and retry logic can be set in the configuration file

```yaml
batching:
  max_rows: 1000  # Flush batch when this many rows are accumulated

retry:
  max_attempts: 3      # Maximum retry attempts (default: 3)
  initial_delay: "1s"  # Initial retry delay (default: "1s")
  max_delay: "30s"     # Maximum retry delay (default: "30s")
```

## HTTP Metrics Endpoint

mysql2bq provides an optional HTTP server for real-time monitoring and health checks:

### Endpoints

- **GET `/metrics`**: Returns comprehensive CDC metrics as JSON
- **GET `/health`**: Returns basic health status and uptime

### Configurations for the HTTP server can be set in the configuration file

```yaml
http:
  enabled: true       # Enable HTTP server (default: false)
  host: "localhost"   # Server host (default: "localhost")
  port: 8080          # Server port (default: 8080)
  path: "/metrics"    # Metrics endpoint path (default: "/metrics")
```

### Metrics Response

The `/metrics` endpoint returns a JSON object with:

- **Global metrics**: Total events/rows processed, rates, uptime, errors
- **Table-specific metrics**: Per-table event counts, processing times, positions
- **Replication status**: Current GTID set or binlog position

Example response:

```json
{
  "total_events_processed": 150,
  "total_rows_processed": 2500,
  "events_per_second": 2.5,
  "rows_per_second": 41.7,
  "current_position": "mysql-bin.000001:12345",
  "uptime": "3000000000000",
  "table_metrics": {
    "users": {
      "events_processed": 100,
      "rows_processed": 2000,
      "last_processed_time": "2023-01-01T12:00:00Z"
    }
  }
}
```

## Asynchronous Checkpointing

mysql2bq implements robust checkpointing to ensure reliable resume capability after restarts or failures:

### Checkpoint Storage

- **GTID Mode**: Stores executed GTID sets for precise resume points
- **Position Mode**: Stores binlog filename and position
- **File-based**: Checkpoints persisted to JSON file for durability

### Asynchronous Processing

- **Non-blocking**: Checkpoint saving runs in background goroutine
- **Buffered Channel**: Prevents blocking during high-throughput periods
- **Periodic Saves**: Checkpoints saved every 100 events or 30 seconds
- **Graceful Shutdown**: Final checkpoint saved during application shutdown

### Resume Behavior

- **Automatic Detection**: Detects GTID vs position-based replication
- **Fallback Logic**: Falls back to position mode if GTID unavailable
- **Error Recovery**: Continues from last valid checkpoint on failures
- **Start from Beginning**: Initializes from earliest binlog if no checkpoint exists

### Configurations for checkpointing can be set in the configuration file

```yaml
checkpoint:
  type: "file"        # Checkpoint storage type
  path: "/path/to/checkpoint.json"  # Checkpoint file location
```

## One-time snapshot + binlog-catchup mode ("once")

You can run a single-shot sync (snapshot the configured tables and then apply binlog events until caught up) using `mode: "once"` or the CLI flag `--mode once`.

Behavior:

- The pipeline captures a binlog/GTID start point, snapshots the configured tables into BigQuery, then streams binlog events from the captured start point until the position/GTID observed immediately after the snapshot is reached.
- This "snapshot + catchup" workflow ensures the target table contains the snapshot state plus any intervening changes.

Usage examples:

```bash
# Run one-time snapshot+catchup using config file
./bin/mysql2bq start --config config.yaml --mode once
```

Configuration example:

```yaml
mode: "once"        # "continuous" (default) or "once"
```

Caveats & recommendations:

- The implementation is designed for correctness but users should be aware of DB activity during snapshot. For high-write or large tables prefer a locking/consistent-snapshot strategy and validate results after the sync.

- `insertId` is used to reduce duplicates, but consider application-level uniqueness or verification checksums for stricter guarantees.

## Testing the one-time sync (QA / Production)

Follow these steps when validating a `mode: "once"` (snapshot + binlog-catchup) run in QA or Production.

### Quick safety checklist ✅
- Backup or snapshot the target BigQuery table (or write to a test dataset).
- Use a dedicated `checkpoint.path` for test runs to avoid interfering with production checkpoint state.
- Ensure the BigQuery service account / credentials are configured and have write access.
- If possible, run first against a staging dataset/table and validate rows/row counts before pointing at production.

### Example QA config (fast, verbose, isolated)

```yaml
mode: "once"
mysql:
  host: "qa-mysql.local"
  port: 3306
  user: "replicator"
  password: "secret"
  server_id: 101
bigquery:
  project: "your-gcp-project"
  dataset: "qa_dataset"
  service_account_key_json: "/secrets/qa-gcp-key.json"
cdc:
  tables:
    - db: "app_db"
      table: "users"
batching:
  max_rows: 100
retry:
  max_attempts: 3
  initial_delay: "1s"
  max_delay: "10s"
checkpoint:
  type: "file"
  path: "/var/lib/mysql2bq/checkpoint-qa.json"
http:
  enabled: true
  host: "0.0.0.0"
  port: 9090
  path: "/metrics"
logging:
  level: "debug"
```

### Example Production config (safe, higher-throughput)

```yaml
mode: "once"
mysql:
  host: "prod-mysql-1.internal"
  port: 3306
  user: "replicator"
  password: "REDACTED"
  server_id: 201
bigquery:
  project: "your-gcp-project"
  dataset: "prod_dataset"
  service_account_key_json: "/secrets/prod-gcp-key.json"
batching:
  max_rows: 2000
retry:
  max_attempts: 5
  initial_delay: "1s"
  max_delay: "30s"
checkpoint:
  type: "file"
  path: "/var/lib/mysql2bq/checkpoint-once-prod.json"
http:
  enabled: true
  host: "0.0.0.0"
  port: 8080
logging:
  level: "info"
```

### CLI & Docker examples

- Run binary (uses config file; `--mode` overrides `config.Mode`):

```bash
./bin/mysql2bq start --config /etc/mysql2bq/config.prod.yaml --mode once
```

- Run with Docker Compose (mount config + credentials into the container):

```bash
docker-compose run --rm -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/prod-gcp-key.json mysql2bq start --config /etc/mysql2bq/config.docker.yaml --mode once
```

### What to watch while the job runs 🔎
- Logs: look for these informational messages in order:
  - `Snapshot start point captured` — snapshot start position recorded
  - `Snapshot complete; will stream binlog events until ...` — stop point captured
  - `Reached stop binlog position; completing one-time sync` or `Reached stop GTID set; completing one-time sync` — catchup complete
  - `One-time sync completed successfully` — full run success
- Metrics: poll the `GET /metrics` endpoint (if enabled) to monitor progress and error counters.
- Checkpoint file: verify `checkpoint.path` shows the stop position/GTID after completion.

### Post-run verification ✅
1. Compare row counts (and a few sample rows or checksums) between source DB and BigQuery target.
2. Inspect BigQuery for duplicate rows (use a unique key in SQL or sample dedupe queries).
3. Confirm checkpoint file has been updated to the stop position/GTID.
4. Review logs for any non-retryable errors — address and re-run if needed.

> Tip: run the `once` sync into a separate BigQuery dataset/table when validating changes — only switch production writes after verification.

### Troubleshooting tips ⚠️
- GTID resume failed / GTIDs purged: the pipeline falls back to position-based start and logs a warning. Verify the checkpoint and re-run if needed.
- Long-running snapshot: prefer using a consistent snapshot/backup method for very large tables to avoid long locks or heavy load.
- Permanent row failures: check BigQuery insert error details (non-retryable rows will be reported) and fix schema/data issues before retrying.

## Usage

Start the replication:

```bash
./bin/mysql2bq start --config config.yaml
```

## Development

- Requires Go 1.22+
- Uses Cobra for CLI
- Dependencies managed with Go modules

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Recent Improvements

✔ Graceful shutdown with SIGINT/SIGTERM  
✔ Config validation at startup  
✔ **Automatic GTID/position-based replication detection**  
✔ Checkpoint store interface with GTID and position support  
✔ Improved CDC reliability foundations  
✔ **BigQuery insertId for idempotent data inserts**
✔ **Batch flush with retry logic for robust data transfer**
✔ **HTTP metrics endpoint for monitoring**
✔ **Asynchronous checkpointing for reliable binlog resume**
