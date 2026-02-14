# mysql2bq

A tool for Change Data Capture (CDC) from MySQL to BigQuery.

## Overview

mysql2bq replicates data changes from MySQL databases to Google BigQuery in real-time using CDC techniques.

## Features

- Real-time data replication from MySQL to BigQuery
- Configurable table selection
- Batching for efficient data transfer
- Checkpointing for resumable operations

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
