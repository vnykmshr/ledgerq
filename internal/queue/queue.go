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
	"sync"
	"time"

	"github.com/vnykmshr/ledgerq/internal/format"
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
		Logger:                   logging.NoopLogger{},    // No logging by default
		MetricsCollector:         metrics.NoopCollector{}, // No metrics by default
	}
}

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
	// (in case segments were written but metadata wasn't synced)
	if recoveredNextMsgID > nextMsgID {
		nextMsgID = recoveredNextMsgID
		// Update metadata with recovered state
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
				// Get position from segment reader
				// For priority index, we use a dummy position (offset as uint32)
				// In a real implementation, we'd track file positions
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
		if q.opts.PriorityStarvationWindow > 0 {
			lowEntry := q.priorityIndex.OldestInPriority(format.PriorityLow)
			if lowEntry != nil {
				age := time.Duration(now - lowEntry.Timestamp)
				if age >= q.opts.PriorityStarvationWindow {
					// Low-priority message has starved, process it first
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

		// Update read position to this message ID if it's newer
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

	// Add buffer for expired messages we might skip
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
func (q *Queue) SeekToMessageID(msgID uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	// Validate message ID range
	if msgID == 0 {
		return fmt.Errorf("message ID must be > 0")
	}

	if msgID >= q.nextMsgID {
		return fmt.Errorf("message ID %d not yet written (next ID: %d)", msgID, q.nextMsgID)
	}

	// Set read position
	q.readMsgID = msgID

	// Update metadata
	if err := q.metadata.SetReadMsgID(q.readMsgID); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	// Record metrics
	q.opts.MetricsCollector.RecordSeek()

	return nil
}

// SeekToTimestamp sets the read position to the first message at or after the given timestamp.
// Uses the segment index for efficient lookup.
// Returns an error if no messages exist at or after the timestamp.
func (q *Queue) SeekToTimestamp(timestamp int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	// Ensure data is synced before seeking
	if err := q.segments.Sync(); err != nil {
		return fmt.Errorf("failed to sync before seek: %w", err)
	}

	// Get all segments
	allSegments := q.segments.GetSegments()
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	// Search through segments for the first message at or after timestamp
	for _, seg := range allSegments {
		reader, err := q.segments.OpenReader(seg.BaseOffset)
		if err != nil {
			continue
		}

		// Try to find entry by timestamp using index
		entry, _, _, err := reader.FindByTimestamp(timestamp)
		_ = reader.Close()

		if err == nil {
			// Found a message! Set read position to this message ID
			q.readMsgID = entry.MsgID

			// Update metadata
			if err := q.metadata.SetReadMsgID(q.readMsgID); err != nil {
				return fmt.Errorf("failed to update metadata: %w", err)
			}

			// Record metrics
			q.opts.MetricsCollector.RecordSeek()

			return nil
		}
	}

	return fmt.Errorf("no messages found at or after timestamp %d", timestamp)
}

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
}

// Stats returns current queue statistics.
func (q *Queue) Stats() *Stats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	segments := q.segments.GetSegments()
	if q.segments.GetActiveSegment() != nil {
		segments = append(segments, q.segments.GetActiveSegment())
	}

	return &Stats{
		TotalMessages:   q.nextMsgID - 1,
		PendingMessages: q.nextMsgID - q.readMsgID,
		NextMessageID:   q.nextMsgID,
		ReadMessageID:   q.readMsgID,
		SegmentCount:    len(segments),
	}
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
