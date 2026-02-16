# mysql2bq

> ⚠️ **WARNING: This is a Proof of Concept (POC) and not production-ready code. Use at your own risk.**

A tool for Change Data Capture (CDC) from MySQL to BigQuery.

## Overview

mysql2bq replicates data changes from MySQL databases to Google BigQuery in real-time using CDC techniques.

## Features

- Real-time data replication from MySQL to BigQuery
- **Automatic GTID or position-based replication detection**
- **Comprehensive metrics tracking for monitoring CDC progress**
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

### Recommended Next Steps

- Implement binlog resume using checkpoint
- Add BigQuery insertId for idempotency
- Implement batch flush retry logic
- Add metrics endpoint
