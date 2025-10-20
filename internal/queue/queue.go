// Package queue provides a persistent message queue implementation.
//
// Queue provides a durable, disk-backed message queue with:
//   - Persistent storage with automatic segment rotation
//   - Message ordering guarantees (FIFO)
//   - Offset-based message tracking
//   - Efficient batching support
//   - Crash recovery
//
// Basic usage:
//
//	q, err := queue.Open("/path/to/queue", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer q.Close()
//
//	// Enqueue a message
//	offset, err := q.Enqueue([]byte("hello"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Dequeue a message
//	msg, err := q.Dequeue()
//	if err != nil {
//	    log.Fatal(err)
//	}
package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/vnykmshr/ledgerq/internal/format"
	"github.com/vnykmshr/ledgerq/internal/logging"
	"github.com/vnykmshr/ledgerq/internal/segment"
)

// Queue is a persistent, disk-backed message queue.
type Queue struct {
	opts *Options

	mu sync.RWMutex

	// Segment manager for storage
	segments *segment.Manager

	// Metadata for persistent state
	metadata *Metadata

	// Message ID tracking
	nextMsgID uint64 // Next message ID to assign
	readMsgID uint64 // Next message ID to read

	// Priority index for priority queue mode (v1.1.0+)
	// Only used when EnablePriorities is true
	priorityIndex *format.PriorityIndex

	// DLQ components (v1.2.0+)
	// Only initialized when DLQPath is configured
	dlq          *Queue        // Separate queue instance for DLQ
	retryTracker *RetryTracker // Retry state tracker

	// Periodic sync
	syncTimer       *time.Timer
	syncTimerActive bool

	// Periodic compaction
	compactionTimer       *time.Timer
	compactionTimerActive bool

	closed bool
}

// Open opens or creates a queue at the specified directory.
// Creates the directory if it doesn't exist.
func Open(dir string, opts *Options) (*Queue, error) {
	if opts == nil {
		opts = DefaultOptions(dir)
	}

	// Validate options for security and correctness
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	// Create directory with restrictive permissions (owner-only)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Check disk space if configured
	if err := checkDiskSpace(dir, opts.MinFreeDiskSpace); err != nil {
		return nil, fmt.Errorf("disk space check failed: %w", err)
	}

	// Open or create metadata file
	metadata, err := OpenMetadata(dir, opts.AutoSync)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata: %w", err)
	}

	// Get state from metadata
	nextMsgID, readMsgID := metadata.GetState()

	// Create segment manager
	segments, err := segment.NewManager(opts.SegmentOptions)
	if err != nil {
		_ = metadata.Close()
		return nil, fmt.Errorf("failed to create segment manager: %w", err)
	}

	// Recover state from segments if needed (for nextMsgID)
	recoveredNextMsgID, _, err := recoverState(segments)
	if err != nil {
		_ = segments.Close()
		_ = metadata.Close()
		return nil, fmt.Errorf("failed to recover queue state: %w", err)
	}

	// Use the higher of metadata nextMsgID and recovered nextMsgID
	// This handles the case where segments were written but metadata wasn't synced
	// before a crash. We always want to use the highest ID to prevent collisions.
	if recoveredNextMsgID > nextMsgID {
		nextMsgID = recoveredNextMsgID
		// Update metadata with recovered state to ensure consistency
		if err := metadata.SetNextMsgID(nextMsgID); err != nil {
			_ = segments.Close()
			_ = metadata.Close()
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}
	}

	q := &Queue{
		opts:      opts,
		segments:  segments,
		metadata:  metadata,
		nextMsgID: nextMsgID,
		readMsgID: readMsgID,
	}

	// Initialize priority index if priority mode is enabled
	if opts.EnablePriorities {
		q.priorityIndex = format.NewPriorityIndex()

		// Rebuild priority index from existing segments
		q.rebuildPriorityIndex()
	}

	// Initialize DLQ if configured (v1.2.0+)
	if opts.DLQPath != "" {
		// Create retry tracker
		retryStatePath := dir + "/.retry_state.json"
		retryTracker, err := NewRetryTracker(retryStatePath, opts.MaxRetries)
		if err != nil {
			_ = metadata.Close()
			_ = segments.Close()
			return nil, fmt.Errorf("failed to create retry tracker: %w", err)
		}
		q.retryTracker = retryTracker

		// Create DLQ queue with modified options to prevent infinite recursion
		// The DLQ itself cannot have a DLQ, and messages in DLQ don't need retry tracking
		dlqOpts := *opts
		dlqOpts.DLQPath = ""                                    // Disable DLQ for the DLQ itself
		dlqOpts.MaxRetries = 0                                  // No retries for DLQ messages
		dlqOpts.SegmentOptions = segment.DefaultManagerOptions(opts.DLQPath) // Use DLQ path for segment manager

		// Open DLQ queue (Open will create directory if it doesn't exist)
		dlq, err := Open(opts.DLQPath, &dlqOpts)
		if err != nil {
			_ = retryTracker.Close()
			_ = metadata.Close()
			_ = segments.Close()
			return nil, fmt.Errorf("failed to open DLQ: %w", err)
		}
		q.dlq = dlq

		opts.Logger.Info("DLQ initialized",
			logging.F("dlq_path", opts.DLQPath),
			logging.F("max_retries", opts.MaxRetries),
		)
	}

	// Start periodic sync timer if configured
	if !opts.AutoSync && opts.SyncInterval > 0 {
		q.startSyncTimer()
	}

	// Start periodic compaction timer if configured
	if opts.CompactionInterval > 0 {
		q.startCompactionTimer()
	}

	// Update initial metrics state
	stats := q.Stats()
	opts.MetricsCollector.UpdateQueueState(
		stats.PendingMessages,
		uint64(stats.SegmentCount),
		stats.NextMessageID,
		stats.ReadMessageID,
	)

	// Log queue opened
	opts.Logger.Info("queue opened",
		logging.F("dir", dir),
		logging.F("next_msg_id", nextMsgID),
		logging.F("read_msg_id", readMsgID),
		logging.F("segments", len(segments.GetSegments())),
	)

	return q, nil
}

// recoverState scans existing segments to determine the next message ID and read position.
func recoverState(segments *segment.Manager) (nextMsgID, readMsgID uint64, err error) {
	allSegments := segments.GetSegments()

	if len(allSegments) == 0 {
		// No segments yet, start from 1
		return 1, 1, nil
	}

	// Find the highest message ID by scanning the last segment
	lastSeg := allSegments[len(allSegments)-1]
	reader, err := segment.NewReader(lastSeg.Path)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open last segment: %w", err)
	}
	defer func() { _ = reader.Close() }()

	var maxMsgID uint64 = 0

	// Scan all entries in the last segment to find max message ID
	err = reader.ScanAll(func(entry *format.Entry, offset uint64) error {
		if entry.MsgID > maxMsgID {
			maxMsgID = entry.MsgID
		}
		return nil
	})

	if err != nil {
		return 0, 0, fmt.Errorf("failed to scan segment: %w", err)
	}

	// Next message ID is one after the max
	nextMsgID = maxMsgID + 1

	// For now, start reading from the beginning
	// In a future phase, we'll track read position in metadata
	readMsgID = 1

	return nextMsgID, readMsgID, nil
}

// rebuildPriorityIndex scans all existing segments and rebuilds the priority index.
// This is called when opening a queue with EnablePriorities=true.
func (q *Queue) rebuildPriorityIndex() {
	allSegments := q.segments.GetSegments()
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	if len(allSegments) == 0 {
		// No segments yet, nothing to rebuild
		return
	}

	// Scan all segments and add entries to priority index
	for _, seg := range allSegments {
		reader, err := segment.NewReader(seg.Path)
		if err != nil {
			// Skip segments that can't be opened (e.g., empty segments)
			q.opts.Logger.Debug("skipping segment during priority index rebuild",
				logging.F("path", seg.Path),
				logging.F("error", err.Error()),
			)
			continue
		}

		// Scan all entries in this segment
		err = reader.ScanAll(func(entry *format.Entry, offset uint64) error {
			// Only index messages that haven't been read yet
			if entry.MsgID >= q.readMsgID && entry.Type == format.EntryTypeData {
				// Get position from segment reader for priority indexing
				// We cast offset to uint32 for the priority index which expects this type
				// This is safe as segment offsets are controlled and won't exceed uint32 limits
				position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
				q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
			}
			return nil
		})

		_ = reader.Close()

		if err != nil {
			// Skip segments with scan errors
			q.opts.Logger.Debug("skipping segment scan during priority index rebuild",
				logging.F("path", seg.Path),
				logging.F("error", err.Error()),
			)
			continue
		}
	}

	q.opts.Logger.Info("priority index rebuilt",
		logging.F("total_entries", q.priorityIndex.Count()),
		logging.F("high_priority", q.priorityIndex.CountByPriority(format.PriorityHigh)),
		logging.F("medium_priority", q.priorityIndex.CountByPriority(format.PriorityMedium)),
		logging.F("low_priority", q.priorityIndex.CountByPriority(format.PriorityLow)),
	)
}

// Enqueue appends a message to the queue.
// Returns the offset where the message was written.
func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID

	// Create entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue failed",
			logging.F("msg_id", msgID),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithTTL appends a message to the queue with a time-to-live duration.
// The message will expire after the specified TTL and will be skipped during dequeue.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithTTL(payload []byte, ttl time.Duration) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	if ttl <= 0 {
		return 0, fmt.Errorf("TTL must be positive")
	}

	msgID := q.nextMsgID
	now := time.Now().UnixNano()

	// Create entry with TTL
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagTTL,
		MsgID:     msgID,
		Timestamp: now,
		ExpiresAt: now + ttl.Nanoseconds(),
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with TTL failed",
			logging.F("msg_id", msgID),
			logging.F("ttl", ttl.String()),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with TTL",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("ttl", ttl.String()),
		logging.F("expires_at", entry.ExpiresAt),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithHeaders appends a message to the queue with key-value metadata headers.
// Headers can be used for routing, tracing, content-type indication, or message classification.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithHeaders(payload []byte, headers map[string]string) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID

	// Create entry with headers (flag will be set by Marshal if headers present)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: time.Now().UnixNano(),
		Headers:   headers,
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with headers failed",
			logging.F("msg_id", msgID),
			logging.F("headers_count", len(headers)),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with headers",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("headers_count", len(headers)),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithOptions appends a message with both TTL and headers.
// This allows combining multiple features in a single enqueue operation.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithOptions(payload []byte, ttl time.Duration, headers map[string]string) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	if ttl <= 0 {
		return 0, fmt.Errorf("TTL must be positive")
	}

	msgID := q.nextMsgID
	now := time.Now().UnixNano()

	// Create entry with both TTL and headers (flags will be set by Marshal)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: now,
		ExpiresAt: now + ttl.Nanoseconds(),
		Headers:   headers,
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with options failed",
			logging.F("msg_id", msgID),
			logging.F("ttl", ttl.String()),
			logging.F("headers_count", len(headers)),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with TTL and headers",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("ttl", ttl.String()),
		logging.F("headers_count", len(headers)),
		logging.F("expires_at", entry.ExpiresAt),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithPriority appends a message with a specific priority level (v1.1.0+).
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithPriority(payload []byte, priority uint8) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	// Validate priority before acquiring lock
	if priority > 2 {
		return 0, fmt.Errorf("invalid priority %d (must be 0-2)", priority)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID

	// Create entry with priority (flag will be set by Marshal if not PriorityLow)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: time.Now().UnixNano(),
		Priority:  priority,
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with priority failed",
			logging.F("msg_id", msgID),
			logging.F("priority", priority),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with priority",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("priority", priority),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithAllOptions appends a message with priority, TTL, and headers (v1.1.0+).
// This is the most flexible enqueue method, combining all available features.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithAllOptions(payload []byte, priority uint8, ttl time.Duration, headers map[string]string) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	// Validate priority before acquiring lock
	if priority > 2 {
		return 0, fmt.Errorf("invalid priority %d (must be 0-2)", priority)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID
	now := time.Now().UnixNano()

	// Create entry with all options (flags will be set by Marshal)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: now,
		Priority:  priority,
		Payload:   payload,
	}

	// Set TTL if specified
	if ttl > 0 {
		entry.ExpiresAt = now + ttl.Nanoseconds()
	}

	// Set headers if specified
	if len(headers) > 0 {
		entry.Headers = headers
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with all options failed",
			logging.F("msg_id", msgID),
			logging.F("priority", priority),
			logging.F("ttl", ttl.String()),
			logging.F("headers_count", len(headers)),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with all options",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("priority", priority),
		logging.F("ttl", ttl.String()),
		logging.F("headers_count", len(headers)),
		logging.F("expires_at", entry.ExpiresAt),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueBatch appends multiple messages to the queue in a single operation.
// This is more efficient than calling Enqueue() multiple times as it performs
// a single fsync for all messages.
// Returns the offsets where the messages were written.
func (q *Queue) EnqueueBatch(payloads [][]byte) ([]uint64, error) {
	start := time.Now()

	if len(payloads) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	// Validate all message sizes before acquiring lock
	for i, payload := range payloads {
		if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return nil, fmt.Errorf("queue is closed")
	}

	offsets := make([]uint64, len(payloads))
	timestamp := time.Now().UnixNano()
	totalBytes := 0

	// Write all entries
	for i, payload := range payloads {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     q.nextMsgID,
			Timestamp: timestamp,
			Payload:   payload,
		}

		offset, err := q.segments.Write(entry)
		if err != nil {
			// On error, sync what we've written so far
			_ = q.segments.Sync()
			q.opts.MetricsCollector.RecordEnqueueError()
			return offsets[:i], fmt.Errorf("failed to write entry %d: %w", i, err)
		}

		// Add to priority index if priority mode is enabled
		if q.opts.EnablePriorities && q.priorityIndex != nil {
			position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
			q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
		}

		offsets[i] = offset
		totalBytes += len(payload)
		q.nextMsgID++
	}

	// Update metadata with final nextMsgID
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offsets, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Single sync for the entire batch
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offsets, fmt.Errorf("failed to sync batch: %w", err)
		}
	}

	q.opts.Logger.Debug("batch enqueued",
		logging.F("count", len(payloads)),
		logging.F("first_msg_id", q.nextMsgID-uint64(len(payloads))),
		logging.F("last_msg_id", q.nextMsgID-1),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueueBatch(len(payloads), totalBytes, time.Since(start))

	return offsets, nil
}

// BatchEnqueueOptions contains options for enqueueing a single message in a batch operation.
type BatchEnqueueOptions struct {
	// Payload is the message data
	Payload []byte

	// Priority is the message priority level (v1.1.0+)
	// Default: PriorityLow (0)
	Priority uint8

	// TTL is the time-to-live duration for the message
	// Set to 0 for no expiration
	// Default: 0 (no expiration)
	TTL time.Duration

	// Headers contains key-value metadata for the message
	// Default: nil
	Headers map[string]string
}

// EnqueueBatchWithOptions appends multiple messages with individual options to the queue.
// This is more efficient than calling EnqueueWithAllOptions() multiple times as it performs
// a single fsync for all messages. Each message can have different priority, TTL, and headers.
// Returns the offsets where the messages were written.
func (q *Queue) EnqueueBatchWithOptions(messages []BatchEnqueueOptions) ([]uint64, error) {
	start := time.Now()

	if len(messages) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	// Validate all messages before acquiring lock
	for i, msg := range messages {
		if msg.Priority > 2 {
			return nil, fmt.Errorf("message %d: invalid priority %d (must be 0-2)", i, msg.Priority)
		}
		if len(msg.Payload) == 0 {
			return nil, fmt.Errorf("message %d: payload cannot be empty", i)
		}
		if err := validateMessageSize(msg.Payload, q.opts.MaxMessageSize); err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return nil, fmt.Errorf("queue is closed")
	}

	offsets := make([]uint64, len(messages))
	timestamp := time.Now().UnixNano()
	totalBytes := 0

	// Write all entries
	for i, msg := range messages {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     q.nextMsgID,
			Timestamp: timestamp,
			Priority:  msg.Priority,
			Payload:   msg.Payload,
		}

		// Set TTL if specified
		if msg.TTL > 0 {
			entry.ExpiresAt = timestamp + msg.TTL.Nanoseconds()
		}

		// Set headers if specified
		if len(msg.Headers) > 0 {
			entry.Headers = msg.Headers
		}

		offset, err := q.segments.Write(entry)
		if err != nil {
			// On error, sync what we've written so far
			_ = q.segments.Sync()
			q.opts.MetricsCollector.RecordEnqueueError()
			return offsets[:i], fmt.Errorf("failed to write entry %d: %w", i, err)
		}

		// Add to priority index if priority mode is enabled
		if q.opts.EnablePriorities && q.priorityIndex != nil {
			position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
			q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
		}

		offsets[i] = offset
		totalBytes += len(msg.Payload)
		q.nextMsgID++
	}

	// Update metadata with final nextMsgID
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offsets, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Single sync for the entire batch
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offsets, fmt.Errorf("failed to sync batch: %w", err)
		}
	}

	q.opts.Logger.Debug("batch enqueued with options",
		logging.F("count", len(messages)),
		logging.F("first_msg_id", q.nextMsgID-uint64(len(messages))),
		logging.F("last_msg_id", q.nextMsgID-1),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueueBatch(len(messages), totalBytes, time.Since(start))

	return offsets, nil
}

// Message represents a dequeued message.
type Message struct {
	// ID is the unique message identifier
	ID uint64

	// Offset is the file offset where the message is stored
	Offset uint64

	// Payload is the message data
	Payload []byte

	// Timestamp is when the message was enqueued (Unix nanoseconds)
	Timestamp int64

	// ExpiresAt is when the message expires (Unix nanoseconds), 0 if no TTL
	ExpiresAt int64

	// Priority is the message priority level (v1.1.0+)
	Priority uint8

	// Headers contains key-value metadata for the message
	Headers map[string]string
}

// Dequeue retrieves the next message from the queue.
// Returns an error if no messages available.
// Automatically skips expired messages (with TTL).
// When EnablePriorities is true, returns messages in priority order (High → Medium → Low).
func (q *Queue) Dequeue() (*Message, error) {
	start := time.Now()

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordDequeueError()
		return nil, fmt.Errorf("queue is closed")
	}

	// Ensure data is flushed to disk before reading
	if err := q.segments.Sync(); err != nil {
		q.opts.MetricsCollector.RecordDequeueError()
		return nil, fmt.Errorf("failed to sync before dequeue: %w", err)
	}

	// Get current time for TTL checking and starvation prevention
	now := time.Now().UnixNano()

	// Check if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		return q.dequeuePriority(now, start)
	}

	// FIFO mode - use existing logic
	return q.dequeueFIFO(now, start)
}

// dequeueFIFO implements FIFO dequeue logic (original behavior)
func (q *Queue) dequeueFIFO(now int64, start time.Time) (*Message, error) {
	// Loop to skip expired messages
	maxAttempts := 1000 // Prevent infinite loop
	attempts := 0

	for attempts < maxAttempts {
		attempts++

		// Check if there are any messages to read
		if q.readMsgID >= q.nextMsgID {
			q.opts.MetricsCollector.RecordDequeueError()
			return nil, fmt.Errorf("no messages available")
		}

		// Find the segment containing the read message ID
		allSegments := q.segments.GetSegments()

		// Add active segment to search
		activeSeg := q.segments.GetActiveSegment()
		if activeSeg != nil {
			allSegments = append(allSegments, activeSeg)
		}

		// Search for the segment containing readMsgID
		found := false
		for _, seg := range allSegments {
			reader, err := q.segments.OpenReader(seg.BaseOffset)
			if err != nil {
				continue
			}

			// Check if this segment might contain our message
			entry, offset, _, err := reader.FindByMessageID(q.readMsgID)
			_ = reader.Close()

			if err == nil {
				found = true

				// Check if message has expired
				if entry.IsExpired(now) {
					// Skip expired message
					q.opts.Logger.Debug("skipping expired message",
						logging.F("msg_id", entry.MsgID),
						logging.F("expires_at", entry.ExpiresAt),
						logging.F("now", now),
					)

					// Advance read position and continue
					q.readMsgID++
					if err := q.metadata.SetReadMsgID(q.readMsgID); err != nil {
						return nil, fmt.Errorf("failed to update metadata: %w", err)
					}
					break // Continue outer loop
				}

				// Found valid (non-expired) message!
				msg := &Message{
					ID:        entry.MsgID,
					Offset:    offset,
					Payload:   entry.Payload,
					Timestamp: entry.Timestamp,
					ExpiresAt: entry.ExpiresAt,
					Priority:  entry.Priority,
					Headers:   entry.Headers,
				}

				// Advance read position
				q.readMsgID++

				// Update metadata
				if err := q.metadata.SetReadMsgID(q.readMsgID); err != nil {
					return msg, fmt.Errorf("failed to update metadata: %w", err)
				}

				// Record metrics
				q.opts.MetricsCollector.RecordDequeue(len(msg.Payload), time.Since(start))

				return msg, nil
			}
		}

		if !found {
			q.opts.MetricsCollector.RecordDequeueError()
			return nil, fmt.Errorf("message ID %d not found", q.readMsgID)
		}
	}

	// Exceeded max attempts (too many consecutive expired messages)
	q.opts.MetricsCollector.RecordDequeueError()
	return nil, fmt.Errorf("exceeded maximum attempts while skipping expired messages")
}

// dequeuePriority implements priority-aware dequeue logic
func (q *Queue) dequeuePriority(now int64, start time.Time) (*Message, error) {
	maxAttempts := 1000
	attempts := 0

	// Get all segments
	allSegments := q.segments.GetSegments()
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	for attempts < maxAttempts {
		attempts++

		// Check for priority index entries
		if q.priorityIndex.Count() == 0 {
			q.opts.MetricsCollector.RecordDequeueError()
			return nil, fmt.Errorf("no messages available")
		}

		// Try priorities in order: High → Medium → Low
		// With starvation prevention for low priority
		var targetOffset uint64
		var targetPriority uint8
		found := false

		// Check for starvation: if low-priority messages have waited too long, promote them
		// This prevents low-priority messages from being starved by a constant stream of high-priority ones
		if q.opts.PriorityStarvationWindow > 0 {
			lowEntry := q.priorityIndex.OldestInPriority(format.PriorityLow)
			if lowEntry != nil {
				age := time.Duration(now - lowEntry.Timestamp)
				if age >= q.opts.PriorityStarvationWindow {
					// Low-priority message has starved, temporarily promote it to prevent indefinite waiting
					targetOffset = lowEntry.Offset
					targetPriority = format.PriorityLow
					found = true
					q.opts.Logger.Debug("promoting starved low-priority message",
						logging.F("offset", targetOffset),
						logging.F("age", age.String()),
					)
				}
			}
		}

		// If no starvation, check priorities in order
		if !found {
			// Try High priority first
			highEntry := q.priorityIndex.OldestInPriority(format.PriorityHigh)
			if highEntry != nil {
				targetOffset = highEntry.Offset
				targetPriority = format.PriorityHigh
				found = true
			} else {
				// Try Medium priority
				mediumEntry := q.priorityIndex.OldestInPriority(format.PriorityMedium)
				if mediumEntry != nil {
					targetOffset = mediumEntry.Offset
					targetPriority = format.PriorityMedium
					found = true
				} else {
					// Try Low priority
					lowEntry := q.priorityIndex.OldestInPriority(format.PriorityLow)
					if lowEntry != nil {
						targetOffset = lowEntry.Offset
						targetPriority = format.PriorityLow
						found = true
					}
				}
			}
		}

		if !found {
			q.opts.MetricsCollector.RecordDequeueError()
			return nil, fmt.Errorf("no messages available")
		}

		// Find and read the entry from segments
		var entry *format.Entry
		var fileOffset uint64
		entryFound := false

		for _, seg := range allSegments {
			reader, err := q.segments.OpenReader(seg.BaseOffset)
			if err != nil {
				continue
			}

			// Search for entry at target offset
			_ = reader.ScanAll(func(e *format.Entry, off uint64) error {
				if off == targetOffset {
					entry = e
					fileOffset = off
					entryFound = true
					return fmt.Errorf("found") // Stop scanning
				}
				return nil
			})

			_ = reader.Close()

			if entryFound {
				break
			}
		}

		if !entryFound {
			// Message not found, remove from index and continue
			q.priorityIndex.Remove(targetOffset)
			q.opts.Logger.Debug("removed missing message from priority index",
				logging.F("offset", targetOffset),
			)
			continue
		}

		// Check if message has expired
		if entry.IsExpired(now) {
			// Remove expired message from index
			q.priorityIndex.Remove(targetOffset)
			q.opts.Logger.Debug("skipping expired priority message",
				logging.F("msg_id", entry.MsgID),
				logging.F("priority", targetPriority),
				logging.F("expires_at", entry.ExpiresAt),
			)
			continue
		}

		// Found valid message!
		msg := &Message{
			ID:        entry.MsgID,
			Offset:    fileOffset,
			Payload:   entry.Payload,
			Timestamp: entry.Timestamp,
			ExpiresAt: entry.ExpiresAt,
			Priority:  entry.Priority,
			Headers:   entry.Headers,
		}

		// Remove from priority index
		q.priorityIndex.Remove(targetOffset)

		// Update read position to this message ID if it's newer than current read position
		// In priority mode, messages may be consumed out of order, so we only advance
		// the read position if this message ID is at or beyond the current read position
		if entry.MsgID >= q.readMsgID {
			q.readMsgID = entry.MsgID + 1
			if err := q.metadata.SetReadMsgID(q.readMsgID); err != nil {
				return msg, fmt.Errorf("failed to update metadata: %w", err)
			}
		}

		// Record metrics
		q.opts.MetricsCollector.RecordDequeue(len(msg.Payload), time.Since(start))

		q.opts.Logger.Debug("dequeued priority message",
			logging.F("msg_id", entry.MsgID),
			logging.F("priority", targetPriority),
			logging.F("offset", fileOffset),
		)

		return msg, nil
	}

	// Exceeded max attempts
	q.opts.MetricsCollector.RecordDequeueError()
	return nil, fmt.Errorf("exceeded maximum attempts while skipping expired messages")
}

// DequeueBatch retrieves up to maxMessages from the queue in a single operation.
// Returns fewer messages if the queue has fewer than maxMessages available.
// Returns an error if no messages are available.
// Automatically skips expired messages (with TTL).
func (q *Queue) DequeueBatch(maxMessages int) ([]*Message, error) {
	start := time.Now()

	if maxMessages <= 0 {
		return nil, fmt.Errorf("maxMessages must be > 0")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordDequeueError()
		return nil, fmt.Errorf("queue is closed")
	}

	// Check if there are any messages to read
	if q.readMsgID >= q.nextMsgID {
		q.opts.MetricsCollector.RecordDequeueError()
		return nil, fmt.Errorf("no messages available")
	}

	// Ensure data is flushed to disk before reading
	if err := q.segments.Sync(); err != nil {
		q.opts.MetricsCollector.RecordDequeueError()
		return nil, fmt.Errorf("failed to sync before dequeue: %w", err)
	}

	// Get current time for TTL checking
	now := time.Now().UnixNano()

	// Determine how many messages we can actually read (upper bound)
	available := q.nextMsgID - q.readMsgID
	maxToCheck := available
	if uint64(maxMessages) < maxToCheck {
		maxToCheck = uint64(maxMessages)
	}

	// Add buffer for expired messages we might skip during batch dequeue
	// We check extra messages beyond the requested count because some may be expired
	// and will be skipped, ensuring we can still return the requested number if possible
	maxToCheck = maxToCheck + 1000 // Check up to 1000 extra in case of expired
	if maxToCheck > available {
		maxToCheck = available
	}

	messages := make([]*Message, 0, maxMessages)
	totalBytes := 0

	// Get all segments
	allSegments := q.segments.GetSegments()
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	// Read messages, skipping expired ones
	checked := uint64(0)
	for checked < maxToCheck && len(messages) < maxMessages {
		targetMsgID := q.readMsgID

		// Check if we've reached the end
		if targetMsgID >= q.nextMsgID {
			break
		}

		// Search for the message in segments
		found := false
		for _, seg := range allSegments {
			reader, err := q.segments.OpenReader(seg.BaseOffset)
			if err != nil {
				continue
			}

			entry, offset, _, err := reader.FindByMessageID(targetMsgID)
			_ = reader.Close()

			if err == nil {
				found = true

				// Check if message has expired
				if entry.IsExpired(now) {
					// Skip expired message
					q.opts.Logger.Debug("skipping expired message in batch",
						logging.F("msg_id", entry.MsgID),
						logging.F("expires_at", entry.ExpiresAt),
					)
					q.readMsgID++
					checked++
					break // Continue to next message
				}

				// Valid message - add to results
				msg := &Message{
					ID:        entry.MsgID,
					Offset:    offset,
					Payload:   entry.Payload,
					Timestamp: entry.Timestamp,
					ExpiresAt: entry.ExpiresAt,
					Priority:  entry.Priority,
					Headers:   entry.Headers,
				}
				messages = append(messages, msg)
				totalBytes += len(msg.Payload)
				q.readMsgID++
				checked++
				break
			}
		}

		if !found {
			// If we can't find a message, stop here
			break
		}
	}

	// Update metadata with new read position
	if err := q.metadata.SetReadMsgID(q.readMsgID); err != nil {
		return messages, fmt.Errorf("failed to update metadata: %w", err)
	}

	if len(messages) == 0 {
		q.opts.MetricsCollector.RecordDequeueError()
		return nil, fmt.Errorf("no messages could be read")
	}

	// Record metrics
	q.opts.MetricsCollector.RecordDequeueBatch(len(messages), totalBytes, time.Since(start))

	return messages, nil
}

// SeekToMessageID sets the read position to a specific message ID.
// Subsequent Dequeue() calls will start reading from this message.
// Returns an error if the message ID is invalid or out of range.

// Sync forces a sync of pending writes to disk.
func (q *Queue) Sync() error {
	q.mu.RLock()
	segments := q.segments
	q.mu.RUnlock()

	if segments == nil {
		return fmt.Errorf("queue is closed")
	}

	return segments.Sync()
}

// Close closes the queue and releases all resources.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}

	// Stop sync timer
	if q.syncTimerActive {
		q.syncTimer.Stop()
		q.syncTimerActive = false
	}

	// Stop compaction timer
	if q.compactionTimerActive {
		q.compactionTimer.Stop()
		q.compactionTimerActive = false
	}

	// Close DLQ components first (v1.2.0+)
	if q.retryTracker != nil {
		if err := q.retryTracker.Close(); err != nil {
			q.opts.Logger.Error("failed to close retry tracker",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	if q.dlq != nil {
		if err := q.dlq.Close(); err != nil {
			q.opts.Logger.Error("failed to close DLQ",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	// Close metadata first
	if q.metadata != nil {
		if err := q.metadata.Close(); err != nil {
			q.opts.Logger.Error("failed to close metadata",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	// Close segments
	if q.segments != nil {
		if err := q.segments.Close(); err != nil {
			q.opts.Logger.Error("failed to close segments",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	q.opts.Logger.Info("queue closed",
		logging.F("total_messages", q.nextMsgID-1),
		logging.F("pending_messages", q.nextMsgID-q.readMsgID),
	)

	q.closed = true
	return nil
}

// IsClosed returns whether the queue has been closed.
func (q *Queue) IsClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}

// startSyncTimer starts the periodic sync timer.
func (q *Queue) startSyncTimer() {
	q.syncTimer = time.AfterFunc(q.opts.SyncInterval, func() {
		q.mu.RLock()
		if !q.closed {
			_ = q.segments.Sync() // Ignore error in background sync
		}
		closed := q.closed
		q.mu.RUnlock()

		// Reschedule if not closed
		if !closed {
			q.mu.Lock()
			if !q.closed {
				q.syncTimer.Reset(q.opts.SyncInterval)
			}
			q.mu.Unlock()
		}
	})
	q.syncTimerActive = true
}

// startCompactionTimer starts the periodic compaction timer.
func (q *Queue) startCompactionTimer() {
	q.compactionTimer = time.AfterFunc(q.opts.CompactionInterval, func() {
		start := time.Now()

		q.mu.RLock()
		if !q.closed {
			// Run compaction in background
			result, err := q.segments.Compact()
			if err != nil {
				q.opts.MetricsCollector.RecordCompactionError()
				q.opts.Logger.Error("background compaction failed",
					logging.F("error", err.Error()),
				)
			} else if result.SegmentsRemoved > 0 {
				q.opts.MetricsCollector.RecordCompaction(result.SegmentsRemoved, result.BytesFreed, time.Since(start))
				q.opts.Logger.Info("background compaction completed",
					logging.F("segments_removed", result.SegmentsRemoved),
					logging.F("bytes_freed", result.BytesFreed),
				)
			}
		}
		closed := q.closed
		q.mu.RUnlock()

		// Reschedule if not closed
		if !closed {
			q.mu.Lock()
			if !q.closed {
				q.compactionTimer.Reset(q.opts.CompactionInterval)
			}
			q.mu.Unlock()
		}
	})
	q.compactionTimerActive = true
}

// Stats returns queue statistics.
type Stats struct {
	// TotalMessages is the total number of messages ever enqueued
	TotalMessages uint64

	// PendingMessages is the number of unread messages
	PendingMessages uint64

	// NextMessageID is the ID that will be assigned to the next enqueued message
	NextMessageID uint64

	// ReadMessageID is the ID of the next message to be dequeued
	ReadMessageID uint64

	// SegmentCount is the number of segments
	SegmentCount int

	// DLQ statistics (v1.2.0+)
	// These fields are populated only when DLQ is enabled

	// DLQMessages is the total number of messages in the DLQ
	DLQMessages uint64

	// DLQPendingMessages is the number of unprocessed messages in the DLQ
	DLQPendingMessages uint64

	// RetryTrackedMessages is the number of messages currently being tracked for retries
	RetryTrackedMessages int
}

// Stats returns current queue statistics.
func (q *Queue) Stats() *Stats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	segments := q.segments.GetSegments()
	if q.segments.GetActiveSegment() != nil {
		segments = append(segments, q.segments.GetActiveSegment())
	}

	stats := &Stats{
		TotalMessages:   q.nextMsgID - 1,
		PendingMessages: q.nextMsgID - q.readMsgID,
		NextMessageID:   q.nextMsgID,
		ReadMessageID:   q.readMsgID,
		SegmentCount:    len(segments),
	}

	// Populate DLQ statistics if DLQ is enabled (v1.2.0+)
	if q.dlq != nil {
		dlqStats := q.dlq.Stats()
		stats.DLQMessages = dlqStats.TotalMessages
		stats.DLQPendingMessages = dlqStats.PendingMessages
	}

	// Populate retry tracking statistics if enabled
	if q.retryTracker != nil {
		stats.RetryTrackedMessages = q.retryTracker.Count()
	}

	return stats
}

// Compact manually triggers compaction of old segments based on retention policy.
// Returns the compaction result with segments removed and bytes freed.
func (q *Queue) Compact() (*segment.CompactionResult, error) {
	start := time.Now()

	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		q.opts.MetricsCollector.RecordCompactionError()
		return nil, fmt.Errorf("queue is closed")
	}

	result, err := q.segments.Compact()
	if err != nil {
		q.opts.MetricsCollector.RecordCompactionError()
		return nil, err
	}

	// Record metrics
	q.opts.MetricsCollector.RecordCompaction(result.SegmentsRemoved, result.BytesFreed, time.Since(start))

	return result, nil
}

// StreamHandler is called for each message in the stream.
// Return an error to stop streaming.
type StreamHandler func(*Message) error

// Stream continuously reads messages from the queue and calls the handler for each message.
// Streaming continues until the context is cancelled, an error occurs, or no more messages are available.
// The handler is called for each message in order.
//
// The Stream method polls for new messages with a configurable interval (100ms by default).
// When a message is available, it's immediately passed to the handler.
// If no messages are available, Stream waits briefly before checking again.
//
// Context cancellation will gracefully stop streaming and return context.Canceled.
// Handler errors will stop streaming and return the handler error.
//
// Example usage:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	err := q.Stream(ctx, func(msg *Message) error {
//	    fmt.Printf("Received: %s\n", msg.Payload)
//	    return nil
//	})
func (q *Queue) Stream(ctx context.Context, handler StreamHandler) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	// Poll interval for checking new messages
	pollInterval := 100 * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled - graceful shutdown
			return ctx.Err()

		case <-ticker.C:
			// Try to dequeue a message
			msg, err := q.Dequeue()
			if err != nil {
				// Check if error is "no messages available"
				// If so, continue polling; otherwise return error
				if err.Error() == "no messages available" {
					continue
				}
				return fmt.Errorf("stream dequeue error: %w", err)
			}

			// Call handler with the message
			if err := handler(msg); err != nil {
				return fmt.Errorf("handler error: %w", err)
			}

			// Reset ticker to immediately check for next message
			// This provides better throughput when messages are available
			ticker.Reset(pollInterval)
		}
	}
}

// Ack acknowledges successful processing of a message (v1.2.0+).
// When DLQ is enabled, this removes the message from retry tracking.
// If DLQ is not configured, this method is a no-op.
//
// This should be called after successfully processing a dequeued message
// to indicate that the message does not need to be retried.
func (q *Queue) Ack(msgID uint64) error {
	q.mu.RLock()
	retryTracker := q.retryTracker
	q.mu.RUnlock()

	// Check if DLQ is enabled
	if retryTracker == nil {
		return nil // No-op if DLQ not configured
	}

	return retryTracker.Ack(msgID)
}

// Nack reports a message processing failure (v1.2.0+).
// When DLQ is enabled, this increments the retry count and potentially moves
// the message to the dead letter queue if max retries are exceeded.
// If DLQ is not configured, this method is a no-op.
//
// The reason parameter should describe why the message processing failed.
// This reason is stored with the retry metadata for debugging purposes.
//
// Returns an error if the operation fails.
func (q *Queue) Nack(msgID uint64, reason string) error {
	q.mu.RLock()
	retryTracker := q.retryTracker
	q.mu.RUnlock()

	// Check if DLQ is enabled
	if retryTracker == nil {
		return nil // No-op if DLQ not configured
	}

	// Record the failure and check if max retries exceeded
	exceeded, err := retryTracker.Nack(msgID, reason)
	if err != nil {
		return fmt.Errorf("failed to record nack: %w", err)
	}

	// If max retries exceeded, move message to DLQ
	if exceeded {
		q.opts.Logger.Info("message exceeded max retries, moving to DLQ",
			logging.F("msg_id", msgID),
			logging.F("reason", reason),
		)

		// Move message to DLQ
		if err := q.moveToDLQ(msgID, reason); err != nil {
			return fmt.Errorf("failed to move message to DLQ: %w", err)
		}
	}

	return nil
}

// moveToDLQ moves a message from the main queue to the DLQ.
// The message is copied to the DLQ with additional metadata headers
// indicating the failure reason and retry count.
func (q *Queue) moveToDLQ(msgID uint64, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Get retry information
	retryInfo := q.retryTracker.GetInfo(msgID)
	if retryInfo == nil {
		return fmt.Errorf("no retry information found for message %d", msgID)
	}

	// Ensure data is synced before searching
	if err := q.segments.Sync(); err != nil {
		return fmt.Errorf("failed to sync before DLQ move: %w", err)
	}

	// Find the message in segments
	allSegments := q.segments.GetSegments()
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	var entry *format.Entry
	found := false

	for _, seg := range allSegments {
		reader, err := q.segments.OpenReader(seg.BaseOffset)
		if err != nil {
			continue
		}

		// Search for the message
		e, _, _, err := reader.FindByMessageID(msgID)
		_ = reader.Close()

		if err == nil {
			entry = e
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("message %d not found in queue segments", msgID)
	}

	// Prepare DLQ headers with retry metadata
	dlqHeaders := make(map[string]string)

	// Copy existing headers if present
	if entry.Headers != nil {
		for k, v := range entry.Headers {
			dlqHeaders[k] = v
		}
	}

	// Add DLQ-specific metadata (sanitize failure reason to prevent information leakage)
	dlqHeaders["dlq.original_msg_id"] = fmt.Sprintf("%d", msgID)
	dlqHeaders["dlq.retry_count"] = fmt.Sprintf("%d", retryInfo.RetryCount)
	dlqHeaders["dlq.failure_reason"] = sanitizeFailureReason(reason)
	dlqHeaders["dlq.last_failure"] = retryInfo.LastFailure.Format("2006-01-02T15:04:05.000Z07:00")

	// Enqueue to DLQ with original payload and enhanced headers
	_, err := q.dlq.EnqueueWithHeaders(entry.Payload, dlqHeaders)
	if err != nil {
		return fmt.Errorf("failed to enqueue message to DLQ: %w", err)
	}

	q.opts.Logger.Info("message moved to DLQ",
		logging.F("msg_id", msgID),
		logging.F("retry_count", retryInfo.RetryCount),
		logging.F("failure_reason", reason),
	)

	// Clean up retry tracking since the message is now in DLQ and won't be retried from main queue
	// We use Ack() to remove the retry tracking entry, as the message has been "processed" (moved to DLQ)
	if err := q.retryTracker.Ack(msgID); err != nil {
		q.opts.Logger.Error("failed to clean up retry tracking after DLQ move",
			logging.F("msg_id", msgID),
			logging.F("error", err.Error()),
		)
		// Don't return error - message is already safely in DLQ, tracking cleanup is non-critical
	}

	return nil
}

// GetDLQ returns the dead letter queue for inspection (v1.2.0+).
// Returns nil if DLQ is not configured.
// The returned queue can be used to inspect or dequeue messages from the DLQ.
func (q *Queue) GetDLQ() *Queue {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.dlq
}

// GetRetryInfo returns retry information for a message (v1.2.0+).
// Returns nil if DLQ is not configured or if the message has no retry tracking.
// This is useful for implementing custom retry logic and backoff strategies.
//
// The returned RetryInfo contains:
//   - MessageID: The message ID being tracked
//   - RetryCount: Number of times Nack() has been called for this message
//   - LastFailure: Timestamp of the most recent Nack() call
//   - FailureReason: Reason string from the most recent Nack() call
//
// Example usage:
//
//	msg, _ := q.Dequeue()
//	info := q.GetRetryInfo(msg.ID)
//	if info != nil && info.RetryCount > 0 {
//	    // Calculate backoff based on retry count
//	    backoff := time.Duration(1<<uint(info.RetryCount)) * time.Second
//	    time.Sleep(backoff)
//	}
func (q *Queue) GetRetryInfo(msgID uint64) *RetryInfo {
	q.mu.RLock()
	retryTracker := q.retryTracker
	q.mu.RUnlock()

	if retryTracker == nil {
		return nil // DLQ not configured
	}

	return retryTracker.GetInfo(msgID)
}

// RequeueFromDLQ moves a message from the DLQ back to the main queue (v1.2.0+).
// The message ID should be from the DLQ (not the original message ID).
// Returns an error if DLQ is not configured or the message is not found.
func (q *Queue) RequeueFromDLQ(dlqMsgID uint64) error {
	q.mu.RLock()
	dlq := q.dlq
	q.mu.RUnlock()

	if dlq == nil {
		return fmt.Errorf("DLQ not configured")
	}

	// Find the message in DLQ
	dlq.mu.Lock()
	allSegments := dlq.segments.GetSegments()
	activeSeg := dlq.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	var entry *format.Entry
	found := false

	for _, seg := range allSegments {
		reader, err := dlq.segments.OpenReader(seg.BaseOffset)
		if err != nil {
			continue
		}

		// Search for the message
		e, _, _, err := reader.FindByMessageID(dlqMsgID)
		_ = reader.Close()

		if err == nil {
			entry = e
			found = true
			break
		}
	}
	dlq.mu.Unlock()

	if !found {
		return fmt.Errorf("message %d not found in DLQ", dlqMsgID)
	}

	// Enqueue back to main queue (preserving headers but removing DLQ metadata)
	headers := make(map[string]string)
	if entry.Headers != nil {
		for k, v := range entry.Headers {
			// Skip DLQ-specific headers when re-queuing
			if k != "dlq.original_msg_id" && k != "dlq.retry_count" &&
				k != "dlq.failure_reason" && k != "dlq.last_failure" {
				headers[k] = v
			}
		}
	}

	// Enqueue to main queue
	var err error
	if len(headers) > 0 {
		_, err = q.EnqueueWithHeaders(entry.Payload, headers)
	} else {
		_, err = q.Enqueue(entry.Payload)
	}

	if err != nil {
		return fmt.Errorf("failed to requeue message to main queue: %w", err)
	}

	q.opts.Logger.Info("message requeued from DLQ",
		logging.F("dlq_msg_id", dlqMsgID),
	)

	return nil
}
