# Seeder Design: Snapshot + CDC Handoff for mysql2bq

## Overview

This document outlines the design for adding snapshot + incremental sync capabilities to mysql2bq, inspired by Gravity's mature CDC system. The goal is to enable full historical data seeding followed by seamless transition to real-time CDC.

## Gap Analysis: What Gravity Has That We Need

Based on analysis of Gravity's capabilities, here are the key features to borrow conceptually:

### 1. Full Snapshot + Incremental Sync

**Problem**: CDC alone only captures new changes, leaving BigQuery tables empty for historical data.

**Solution**: Implement backfill mode that:

- Performs initial full table copy
- Records binlog position at snapshot start
- Seamlessly switches to CDC from that position

**CLI Commands**:

```bash
mysql2bq backfill --table users
mysql2bq start --resume
```

**Implementation Pattern**:

1. SELECT rows in PK order
2. Write to BigQuery with insertId for idempotency
3. Record checkpoint (binlog position/GTID)
4. Switch to CDC mode

### 2. Transaction-Aware Event Handling

**Problem**: Partial transaction writes can cause inconsistent state in BigQuery.

**Solution**: Buffer events until COMMIT in binlog stream.

**Key Requirements**:

- Group row events by transaction
- Prevent flushing mid-transaction
- Preserve correct ordering

### 3. Built-in Transformation Layer

**Problem**: Minimal transformation capabilities limit flexibility.

**Solution**: Config-driven mapping:

```yaml
tables:
  - db: app
    table: users
    rename:
      created_at: createdAt
    drop:
      - internal_flag
```

### 4. Flow Control + Backpressure

**Problem**: CDC readers can outrun BigQuery, causing failures.

**Solution**: Add:

- Bounded channel buffers
- Retry queue with exponential backoff
- Rate limiting
- Writer worker pool

### 5. Operational Visibility

**Problem**: Lack of monitoring makes debugging hard.

**Solution**: Expose metrics:

- rows_processed_total
- batch_write_errors_total
- replication_lag_seconds

## Implementation Phases

### Phase 1: Correctness

- ✅ Transaction buffering
- ✅ Checkpoint-after-write guarantee

### Phase 2: Completeness ✅

- ✅ Full snapshot/backfill command
- ✅ Delete handling strategy

### Phase 3: Resilience ✅

- ✅ Enhanced retry logic with backpressure
- ✅ Advanced metrics and monitoring  
- ✅ Transaction-aware event buffering
- ✅ Schema auto-creation

### Phase 4: Usability

- ✅ Transformation config

## Architecture

### Components

1. **Snapshot Scanner**: Reads full table data in PK order
2. **CDC Streamer**: Processes binlog events
3. **Transaction Buffer**: Groups events by transaction
4. **BigQuery Writer**: Handles idempotent inserts with retry
5. **Checkpoint Manager**: Tracks progress for resumability
6. **Metrics Exporter**: Provides operational visibility

### Data Flow

```bash
MySQL Table → Snapshot Scanner → Transaction Buffer → BigQuery Writer
                    ↓
MySQL Binlog → CDC Streamer → Transaction Buffer → BigQuery Writer
                              ↓
                    Checkpoint Manager ← Metrics Exporter
```

### Checkpoint Strategy

- **Snapshot Phase**: Record binlog position before starting scan
- **Handoff Phase**: Use recorded position to start CDC
- **Resume**: Support both GTID and position-based checkpoints

### Failure Handling

- **Snapshot Failures**: Retry from last committed checkpoint
- **CDC Failures**: Reconnect and resume from checkpoint
- **BigQuery Errors**: Exponential backoff with dead letter queue

## API Surface

### CLI Commands

```bash
# Backfill a specific table ✅
mysql2bq backfill --table users --database app

# Backfill multiple tables ✅
mysql2bq backfill --config backfill.yaml

# Start CDC with resume
mysql2bq start --resume

# Check status
mysql2bq status
```

### Configuration

```yaml
backfill:
  tables:
    - database: app
      table: users
      batch_size: 1000
      where: "created_at > '2023-01-01'"

cdc:
  resume_from_checkpoint: true
  transaction_buffer_size: 1000

bigquery:
  dataset: my_dataset
  table_prefix: ""
```

## Code Structure

```bash
internal/seeder/
├── backfill/
│   ├── scanner.go      # Table scanning logic
│   ├── batcher.go      # Batch processing
│   └── checkpoint.go   # Progress tracking
├── cdc/
│   ├── streamer.go     # Binlog processing
│   ├── transaction.go  # Transaction grouping
│   └── event.go        # Event handling
├── writer/
│   ├── bigquery.go     # BigQuery client
│   ├── retry.go        # Retry logic
│   └── metrics.go      # Metrics collection
└── config/
    └── config.go       # Configuration structs
```

## Testing Strategy

- **Unit Tests**: Each component in isolation
- **Integration Tests**: End-to-end snapshot + CDC flow
- **Performance Tests**: Large table backfill scenarios

## Migration Path

1. Extract existing CDC logic into shared components
2. Implement snapshot scanner reusing Gravity patterns
3. Add transaction buffering to existing pipeline
4. Implement backfill command
5. Add metrics and monitoring
6. Update configuration schema

This design makes mysql2bq a production-grade ingestion system while maintaining its BigQuery-focused architecture
