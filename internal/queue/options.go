// Package queue provides configuration and validation for queue options.
// This file contains the Options struct and related functions.
package queue

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/vnykmshr/ledgerq/internal/logging"
	"github.com/vnykmshr/ledgerq/internal/metrics"
	"github.com/vnykmshr/ledgerq/internal/segment"
)

// Options configures queue behavior.
type Options struct {
	// SegmentOptions configures segment management
	SegmentOptions *segment.ManagerOptions

	// AutoSync enables automatic syncing after each write
	AutoSync bool

	// SyncInterval for periodic syncing (if AutoSync is false)
	SyncInterval time.Duration

	// CompactionInterval for automatic background compaction (0 = disabled)
	CompactionInterval time.Duration

	// EnablePriorities enables priority queue mode (v1.1.0+)
	// When disabled, all messages are treated as PriorityLow (FIFO behavior)
	EnablePriorities bool

	// PriorityStarvationWindow prevents low-priority message starvation (v1.1.0+)
	// Low-priority messages waiting longer than this duration will be promoted
	// Set to 0 to disable starvation prevention
	PriorityStarvationWindow time.Duration

	// DLQPath is the path to the dead letter queue directory (v1.2.0+)
	// If empty, DLQ is disabled. Messages that fail processing after MaxRetries
	// will be moved to this separate queue for inspection and potential reprocessing.
	DLQPath string

	// MaxRetries is the maximum number of delivery attempts before moving to DLQ (v1.2.0+)
	// Set to 0 for unlimited retries (messages never move to DLQ).
	// Only effective when DLQPath is configured.
	// Default: 3
	MaxRetries int

	// MaxMessageSize is the maximum size in bytes for a single message payload (v1.2.0+)
	// Messages larger than this will be rejected during enqueue.
	// Set to 0 for unlimited message size (not recommended for production).
	// Default: 10 MB
	MaxMessageSize int64

	// MinFreeDiskSpace is the minimum required free disk space in bytes (v1.2.0+)
	// Enqueue operations will fail if available disk space falls below this threshold.
	// Set to 0 to disable disk space checking (not recommended for production).
	// Default: 100 MB
	MinFreeDiskSpace int64

	// DLQMaxAge is the maximum age for messages in the DLQ (v1.2.0+)
	// Messages older than this duration will be removed during compaction.
	// Set to 0 to keep DLQ messages indefinitely.
	// Default: 0 (no age-based cleanup)
	DLQMaxAge time.Duration

	// DLQMaxSize is the maximum total size in bytes for the DLQ (v1.2.0+)
	// When DLQ exceeds this size, oldest messages will be removed during compaction.
	// Set to 0 for unlimited DLQ size.
	// Default: 0 (no size limit)
	DLQMaxSize int64

	// Logger for structured logging (nil = no logging)
	Logger logging.Logger

	// MetricsCollector for collecting queue metrics (nil = no metrics)
	MetricsCollector MetricsCollector
}

// MetricsCollector defines the interface for recording queue metrics.
type MetricsCollector interface {
	RecordEnqueue(payloadSize int, duration time.Duration)
	RecordDequeue(payloadSize int, duration time.Duration)
	RecordEnqueueBatch(count, totalPayloadSize int, duration time.Duration)
	RecordDequeueBatch(count, totalPayloadSize int, duration time.Duration)
	RecordEnqueueError()
	RecordDequeueError()
	RecordSeek()
	RecordCompaction(segmentsRemoved int, bytesFreed int64, duration time.Duration)
	RecordCompactionError()
	UpdateQueueState(pending, segments, nextMsgID, readMsgID uint64)
}

// DefaultOptions returns sensible defaults for queue configuration.
func DefaultOptions(dir string) *Options {
	return &Options{
		SegmentOptions:           segment.DefaultManagerOptions(dir),
		AutoSync:                 false,
		SyncInterval:             1 * time.Second,
		CompactionInterval:       0,                       // Disabled by default
		EnablePriorities:         false,                   // FIFO mode by default
		PriorityStarvationWindow: 30 * time.Second,        // 30 seconds
		DLQPath:                  "",                      // DLQ disabled by default
		MaxRetries:               3,                       // 3 retries before moving to DLQ
		MaxMessageSize:           10 * 1024 * 1024,        // 10 MB max message size
		MinFreeDiskSpace:         100 * 1024 * 1024,       // 100 MB minimum free space
		DLQMaxAge:                0,                       // No age-based cleanup by default
		DLQMaxSize:               0,                       // No size limit by default
		Logger:                   logging.NoopLogger{},    // No logging by default
		MetricsCollector:         metrics.NoopCollector{}, // No metrics by default
	}
}

// Validate checks if the options are valid and safe to use.
// This method performs security validations including path traversal checks.
func (o *Options) Validate() error {
	if o.SegmentOptions == nil {
		return fmt.Errorf("segment options cannot be nil")
	}

	// Validate queue directory path
	dir := o.SegmentOptions.Directory
	if dir == "" {
		return fmt.Errorf("queue directory path cannot be empty")
	}

	// Check for path traversal patterns
	if strings.Contains(dir, "..") {
		return fmt.Errorf("path traversal not allowed in queue directory: %s", dir)
	}

	// Convert to absolute path for validation
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path for queue directory: %w", err)
	}

	// Clean the path to remove any unusual patterns
	cleanDir := filepath.Clean(absDir)
	if cleanDir != absDir {
		return fmt.Errorf("queue directory path contains unusual patterns: %s", dir)
	}

	// Validate DLQ path if configured
	if o.DLQPath != "" {
		// Check for path traversal patterns
		if strings.Contains(o.DLQPath, "..") {
			return fmt.Errorf("path traversal not allowed in DLQ path: %s", o.DLQPath)
		}

		// Convert to absolute path for validation
		absDLQPath, err := filepath.Abs(o.DLQPath)
		if err != nil {
			return fmt.Errorf("failed to resolve absolute path for DLQ: %w", err)
		}

		// Clean the path
		cleanDLQPath := filepath.Clean(absDLQPath)
		if cleanDLQPath != absDLQPath {
			return fmt.Errorf("DLQ path contains unusual patterns: %s", o.DLQPath)
		}

		// Ensure DLQ path is not the same as queue path
		if cleanDLQPath == cleanDir {
			return fmt.Errorf("DLQ path cannot be the same as queue path")
		}

		// Ensure DLQ is not a subdirectory of queue path or vice versa
		if strings.HasPrefix(cleanDLQPath+string(filepath.Separator), cleanDir+string(filepath.Separator)) {
			return fmt.Errorf("DLQ path cannot be a subdirectory of queue path")
		}
		if strings.HasPrefix(cleanDir+string(filepath.Separator), cleanDLQPath+string(filepath.Separator)) {
			return fmt.Errorf("queue path cannot be a subdirectory of DLQ path")
		}
	}

	// Validate message size limit
	if o.MaxMessageSize < 0 {
		return fmt.Errorf("max message size cannot be negative")
	}

	// Validate disk space limit
	if o.MinFreeDiskSpace < 0 {
		return fmt.Errorf("min free disk space cannot be negative")
	}

	// Validate DLQ size limit
	if o.DLQMaxSize < 0 {
		return fmt.Errorf("DLQ max size cannot be negative")
	}

	return nil
}
