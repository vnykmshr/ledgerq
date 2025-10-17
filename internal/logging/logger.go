// Package logging provides logging interfaces and utilities for LedgerQ.
package logging

import (
	"fmt"
	"log"
	"os"
)

// Level represents the severity of a log message.
type Level int

const (
	// LevelDebug for detailed debugging information
	LevelDebug Level = iota
	// LevelInfo for informational messages
	LevelInfo
	// LevelWarn for warning messages
	LevelWarn
	// LevelError for error messages
	LevelError
)

// String returns the string representation of the log level.
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger is the interface for logging in LedgerQ.
// Users can implement this interface to integrate with their logging system.
type Logger interface {
	// Debug logs a debug message
	Debug(msg string, fields ...Field)

	// Info logs an informational message
	Info(msg string, fields ...Field)

	// Warn logs a warning message
	Warn(msg string, fields ...Field)

	// Error logs an error message
	Error(msg string, fields ...Field)
}

// Field represents a structured logging field.
type Field struct {
	Key   string
	Value interface{}
}

// F is a convenience function to create a Field.
func F(key string, value interface{}) Field {
	return Field{Key: key, Value: value}
}

// NoopLogger is a logger that does nothing.
type NoopLogger struct{}

// Debug implements Logger.
func (NoopLogger) Debug(string, ...Field) {}

// Info implements Logger.
func (NoopLogger) Info(string, ...Field) {}

// Warn implements Logger.
func (NoopLogger) Warn(string, ...Field) {}

// Error implements Logger.
func (NoopLogger) Error(string, ...Field) {}

// DefaultLogger is a simple logger that writes to stdout/stderr.
type DefaultLogger struct {
	minLevel Level
	logger   *log.Logger
}

// NewDefaultLogger creates a new default logger with the specified minimum level.
func NewDefaultLogger(minLevel Level) *DefaultLogger {
	return &DefaultLogger{
		minLevel: minLevel,
		logger:   log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Debug implements Logger.
func (l *DefaultLogger) Debug(msg string, fields ...Field) {
	if l.minLevel <= LevelDebug {
		l.log(LevelDebug, msg, fields...)
	}
}

// Info implements Logger.
func (l *DefaultLogger) Info(msg string, fields ...Field) {
	if l.minLevel <= LevelInfo {
		l.log(LevelInfo, msg, fields...)
	}
}

// Warn implements Logger.
func (l *DefaultLogger) Warn(msg string, fields ...Field) {
	if l.minLevel <= LevelWarn {
		l.log(LevelWarn, msg, fields...)
	}
}

// Error implements Logger.
func (l *DefaultLogger) Error(msg string, fields ...Field) {
	if l.minLevel <= LevelError {
		l.log(LevelError, msg, fields...)
	}
}

func (l *DefaultLogger) log(level Level, msg string, fields ...Field) {
	if len(fields) == 0 {
		l.logger.Printf("[%s] %s", level, msg)
		return
	}

	// Format fields
	fieldStr := ""
	for i, f := range fields {
		if i > 0 {
			fieldStr += " "
		}
		fieldStr += f.Key + "="
		switch v := f.Value.(type) {
		case string:
			fieldStr += v
		default:
			fieldStr += fmt.Sprint(v)
		}
	}

	l.logger.Printf("[%s] %s %s", level, msg, fieldStr)
}
