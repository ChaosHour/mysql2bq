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
