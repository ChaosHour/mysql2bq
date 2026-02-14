package logging

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
)

// Level represents the logging level
type Level int

const (
	ERROR Level = iota
	WARN
	INFO
	DEBUG
)

// Logger provides structured logging for the CDC pipeline
type Logger struct {
	errorLogger *log.Logger
	warnLogger  *log.Logger
	infoLogger  *log.Logger
	debugLogger *log.Logger
	level       Level
}

// NewLogger creates a new logger with the specified level
func NewLogger(level Level, logFile string) (*Logger, error) {
	var writers []io.Writer

	// Always write to stdout
	writers = append(writers, os.Stdout)

	// If log file is specified, also write to file
	if logFile != "" {
		// Create log directory if it doesn't exist
		logDir := filepath.Dir(logFile)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return nil, err
		}

		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		writers = append(writers, file)
	}

	// Create multi-writer
	multiWriter := io.MultiWriter(writers...)

	// Check if stdout is a terminal for colored output
	isTerminal := color.Output == os.Stdout && color.NoColor == false

	// Create loggers with different prefixes and colors
	var errorLogger, warnLogger, infoLogger, debugLogger *log.Logger

	if isTerminal {
		// Colored output for terminal
		errorLogger = log.New(multiWriter, color.RedString("[ERROR] "), log.LstdFlags|log.Lshortfile)
		warnLogger = log.New(multiWriter, color.YellowString("[WARN]  "), log.LstdFlags|log.Lshortfile)
		infoLogger = log.New(multiWriter, color.CyanString("[INFO]  "), log.LstdFlags)
		debugLogger = log.New(multiWriter, color.MagentaString("[DEBUG] "), log.LstdFlags|log.Lshortfile)
	} else {
		// Plain text for files or non-terminal output
		errorLogger = log.New(multiWriter, "[ERROR] ", log.LstdFlags|log.Lshortfile)
		warnLogger = log.New(multiWriter, "[WARN]  ", log.LstdFlags|log.Lshortfile)
		infoLogger = log.New(multiWriter, "[INFO]  ", log.LstdFlags)
		debugLogger = log.New(multiWriter, "[DEBUG] ", log.LstdFlags|log.Lshortfile)
	}

	logger := &Logger{
		errorLogger: errorLogger,
		warnLogger:  warnLogger,
		infoLogger:  infoLogger,
		debugLogger: debugLogger,
		level:       level,
	}

	return logger, nil
}

// Error logs error messages
func (l *Logger) Error(format string, v ...interface{}) {
	if l.level >= ERROR {
		l.errorLogger.Printf(format, v...)
	}
}

// Warn logs warning messages
func (l *Logger) Warn(format string, v ...interface{}) {
	if l.level >= WARN {
		l.warnLogger.Printf(format, v...)
	}
}

// Info logs info messages
func (l *Logger) Info(format string, v ...interface{}) {
	if l.level >= INFO {
		l.infoLogger.Printf(format, v...)
	}
}

// Debug logs debug messages
func (l *Logger) Debug(format string, v ...interface{}) {
	if l.level >= DEBUG {
		l.debugLogger.Printf(format, v...)
	}
}

// LogConnectionIssue logs network connectivity problems
func (l *Logger) LogConnectionIssue(component string, err error, retryCount int) {
	l.Error("Connection issue with %s: %v (retry %d)", component, err, retryCount)
}

// LogDataIssue logs data processing problems
func (l *Logger) LogDataIssue(table string, operation string, err error, recordCount int) {
	l.Error("Data issue in table %s during %s: %v (records affected: %d)", table, operation, err, recordCount)
}

// LogPerformance logs performance metrics
func (l *Logger) LogPerformance(operation string, duration time.Duration, recordCount int) {
	l.Info("Performance: %s completed in %v for %d records (%.2f records/sec)",
		operation, duration, recordCount, float64(recordCount)/duration.Seconds())
}

// LogPipelineEvent logs pipeline lifecycle events
func (l *Logger) LogPipelineEvent(event string, details string) {
	l.Info("Pipeline event: %s - %s", event, details)
}

// LogBigQueryOperation logs BigQuery-specific operations
func (l *Logger) LogBigQueryOperation(operation string, table string, err error) {
	if err != nil {
		l.Error("BigQuery %s failed for table %s: %v", operation, table, err)
	} else {
		l.Debug("BigQuery %s succeeded for table %s", operation, table)
	}
}

// LogMySQLOperation logs MySQL-specific operations
func (l *Logger) LogMySQLOperation(operation string, table string, position string, err error) {
	if err != nil {
		l.Error("MySQL %s failed for table %s at position %s: %v", operation, table, position, err)
	} else {
		l.Debug("MySQL %s succeeded for table %s at position %s", operation, table, position)
	}
}

// LogNetworkLatency logs network latency issues
func (l *Logger) LogNetworkLatency(operation string, latency time.Duration, threshold time.Duration) {
	if latency > threshold {
		l.Warn("High network latency for %s: %v (threshold: %v)", operation, latency, threshold)
	} else {
		l.Debug("Network latency for %s: %v", operation, latency)
	}
}

// LogBatchOperation logs batch processing information
func (l *Logger) LogBatchOperation(table string, batchSize int, processingTime time.Duration) {
	l.Info("Batch processed for table %s: %d records in %v", table, batchSize, processingTime)
}

// LogCheckpoint logs checkpoint operations
func (l *Logger) LogCheckpoint(table string, position string) {
	l.Debug("Checkpoint saved for table %s at position %s", table, position)
}

// CDCStart logs the start of CDC operations
func (l *Logger) CDCStart(message string) {
	l.Info("🚀 CDC START: %s", message)
}

// CDCProgress logs CDC progress information
func (l *Logger) CDCProgress(message string, recordCount int) {
	l.Info("📊 CDC PROGRESS: %s (%d records)", message, recordCount)
}

// CDCError logs CDC-specific errors
func (l *Logger) CDCError(message string, err error) {
	l.Error("❌ CDC ERROR: %s - %v", message, err)
}

// CDCComplete logs successful completion of CDC operations
func (l *Logger) CDCComplete(message string) {
	l.Info("✅ CDC COMPLETE: %s", message)
}

// SetLevel changes the logging level
func (l *Logger) SetLevel(level Level) {
	l.level = level
}

// parseLogLevel converts string log level to Level enum
func parseLogLevel(level string) Level {
	switch strings.ToLower(level) {
	case "debug":
		return DEBUG
	case "info":
		return INFO
	case "warn", "warning":
		return WARN
	case "error":
		return ERROR
	default:
		return INFO // default to info level
	}
}
