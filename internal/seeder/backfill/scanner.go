package backfill

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/checkpoint"
	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
)

// TableScanner handles scanning full tables for backfill operations
type TableScanner struct {
	cfg    *config.Config
	logger *logging.Logger
}

// NewTableScanner creates a new table scanner
func NewTableScanner(cfg *config.Config, logger *logging.Logger) *TableScanner {
	return &TableScanner{
		cfg:    cfg,
		logger: logger,
	}
}

// ScanTable performs a full table scan and emits rows via callback
func (s *TableScanner) ScanTable(ctx context.Context, dbName, tableName string, emitter RowEmitter) error {
	s.logger.Info("Starting full table scan for %s.%s", dbName, tableName)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		s.cfg.MySQL.User, s.cfg.MySQL.Password, s.cfg.MySQL.Host, s.cfg.MySQL.Port, dbName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open MySQL connection for %s.%s: %w", dbName, tableName, err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query table %s.%s: %w", dbName, tableName, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to read columns for %s.%s: %w", dbName, tableName, err)
	}

	batchSize := s.cfg.Batching.MaxRows
	if batchSize <= 0 {
		batchSize = 1000 // default
	}

	var batch []map[string]interface{}
	totalRows := 0

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}

		if err := rows.Scan(valPtrs...); err != nil {
			return fmt.Errorf("failed to scan row for %s.%s: %w", dbName, tableName, err)
		}

		rowMap := make(map[string]interface{})
		for i, col := range cols {
			v := vals[i]
			if b, ok := v.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = v
			}
		}

		batch = append(batch, rowMap)
		totalRows++

		// Emit batch when it reaches the configured size
		if len(batch) >= batchSize {
			if err := emitter.Emit(ctx, batch, tableName); err != nil {
				return fmt.Errorf("failed to emit batch for %s.%s: %w", dbName, tableName, err)
			}
			batch = batch[:0] // reset batch
		}
	}

	// Emit remaining rows
	if len(batch) > 0 {
		if err := emitter.Emit(ctx, batch, tableName); err != nil {
			return fmt.Errorf("failed to emit final batch for %s.%s: %w", dbName, tableName, err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows for %s.%s: %w", dbName, tableName, err)
	}

	s.logger.Info("Completed full table scan for %s.%s: %d rows", dbName, tableName, totalRows)
	return nil
}

// RowEmitter interface for emitting scanned rows
type RowEmitter interface {
	Emit(ctx context.Context, rows []map[string]interface{}, table string) error
}

// PositionRecorder records binlog position for backfill start
type PositionRecorder struct {
	cfg    *config.Config
	logger *logging.Logger
}

// NewPositionRecorder creates a new position recorder
func NewPositionRecorder(cfg *config.Config, logger *logging.Logger) *PositionRecorder {
	return &PositionRecorder{
		cfg:    cfg,
		logger: logger,
	}
}

// RecordStartPosition captures the binlog position before backfill starts
func (r *PositionRecorder) RecordStartPosition(ctx context.Context) (*mysql.Position, mysql.GTIDSet, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql",
		r.cfg.MySQL.User, r.cfg.MySQL.Password, r.cfg.MySQL.Host, r.cfg.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	// Check GTID mode
	var gtidMode string
	err = db.QueryRowContext(ctx, "SELECT @@global.gtid_mode").Scan(&gtidMode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query GTID mode: %w", err)
	}

	gtidEnabled := (gtidMode == "ON")

	var pos *mysql.Position
	var gtidSet mysql.GTIDSet

	if gtidEnabled {
		// Get current GTID set
		var gtidStr string
		err = db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&gtidStr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get GTID executed: %w", err)
		}

		gtidSet, err = mysql.ParseGTIDSet("mysql", gtidStr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse GTID set: %w", err)
		}
	} else {
		// Get current binlog position
		var file string
		var position uint32
		err = db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &position, nil, nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get binlog position: %w", err)
		}

		pos = &mysql.Position{
			Name: file,
			Pos:  position,
		}
	}

	r.logger.Info("Recorded backfill start position: %v", func() interface{} {
		if gtidEnabled {
			return gtidSet.String()
		}
		return pos.String()
	}())

	return pos, gtidSet, nil
}

// SaveCheckpoint saves the start position to checkpoint file
func (r *PositionRecorder) SaveCheckpoint(pos *mysql.Position, gtidSet mysql.GTIDSet) error {
	store := checkpoint.NewFileStore(r.cfg.Checkpoint.Path)
	cp := &checkpoint.Checkpoint{Timestamp: time.Now().Unix()}

	if gtidSet != nil {
		cp.GTIDSet = gtidSet.String()
	} else if pos != nil {
		cp.Position = pos
	}

	if err := store.Save(cp); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	r.logger.Info("Saved backfill start checkpoint")
	return nil
}
