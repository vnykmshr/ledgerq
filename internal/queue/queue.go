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
	"fmt"
	"sync"
	"time"

	"github.com/vnykmshr/ledgerq/internal/format"
	"github.com/vnykmshr/ledgerq/internal/logging"
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

	// Logger for structured logging (nil = no logging)
	Logger logging.Logger
}

// DefaultOptions returns sensible defaults for queue configuration.
func DefaultOptions(dir string) *Options {
	return &Options{
		SegmentOptions: segment.DefaultManagerOptions(dir),
		AutoSync:       false,
		SyncInterval:   1 * time.Second,
		Logger:         logging.NoopLogger{}, // No logging by default
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

	// Periodic sync
	syncTimer       *time.Timer
	syncTimerActive bool

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

	// Start periodic sync timer if configured
	if !opts.AutoSync && opts.SyncInterval > 0 {
		q.startSyncTimer()
	}

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

// Enqueue appends a message to the queue.
// Returns the offset where the message was written.
func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
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
		q.opts.Logger.Error("enqueue failed",
			logging.F("msg_id", msgID),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
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

	return offset, nil
}

// EnqueueBatch appends multiple messages to the queue in a single operation.
// This is more efficient than calling Enqueue() multiple times as it performs
// a single fsync for all messages.
// Returns the offsets where the messages were written.
func (q *Queue) EnqueueBatch(payloads [][]byte) ([]uint64, error) {
	if len(payloads) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, fmt.Errorf("queue is closed")
	}

	offsets := make([]uint64, len(payloads))
	timestamp := time.Now().UnixNano()

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
			return offsets[:i], fmt.Errorf("failed to write entry %d: %w", i, err)
		}

		offsets[i] = offset
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
}

// Dequeue retrieves the next message from the queue.
// Returns an error if no messages are available.
func (q *Queue) Dequeue() (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, fmt.Errorf("queue is closed")
	}

	// Check if there are any messages to read
	if q.readMsgID >= q.nextMsgID {
		return nil, fmt.Errorf("no messages available")
	}

	// Ensure data is flushed to disk before reading
	if err := q.segments.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync before dequeue: %w", err)
	}

	// Find the segment containing the read message ID
	allSegments := q.segments.GetSegments()

	// Add active segment to search
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	// Search for the segment containing readMsgID
	for _, seg := range allSegments {
		reader, err := q.segments.OpenReader(seg.BaseOffset)
		if err != nil {
			continue
		}

		// Check if this segment might contain our message
		// We'll do a simple scan approach for now
		entry, offset, _, err := reader.FindByMessageID(q.readMsgID)
		_ = reader.Close()

		if err == nil {
			// Found the message!
			msg := &Message{
				ID:        entry.MsgID,
				Offset:    offset,
				Payload:   entry.Payload,
				Timestamp: entry.Timestamp,
			}

			// Advance read position
			q.readMsgID++

			// Update metadata
			if err := q.metadata.SetReadMsgID(q.readMsgID); err != nil {
				return msg, fmt.Errorf("failed to update metadata: %w", err)
			}

			return msg, nil
		}
	}

	return nil, fmt.Errorf("message ID %d not found", q.readMsgID)
}

// DequeueBatch retrieves up to maxMessages from the queue in a single operation.
// Returns fewer messages if the queue has fewer than maxMessages available.
// Returns an error if no messages are available.
func (q *Queue) DequeueBatch(maxMessages int) ([]*Message, error) {
	if maxMessages <= 0 {
		return nil, fmt.Errorf("maxMessages must be > 0")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, fmt.Errorf("queue is closed")
	}

	// Check if there are any messages to read
	if q.readMsgID >= q.nextMsgID {
		return nil, fmt.Errorf("no messages available")
	}

	// Ensure data is flushed to disk before reading
	if err := q.segments.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync before dequeue: %w", err)
	}

	// Determine how many messages we can actually read
	available := q.nextMsgID - q.readMsgID
	count := uint64(maxMessages)
	if available < count {
		count = available
	}

	messages := make([]*Message, 0, count)

	// Get all segments
	allSegments := q.segments.GetSegments()
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	// Read messages
	for i := uint64(0); i < count; i++ {
		targetMsgID := q.readMsgID + i

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
				msg := &Message{
					ID:        entry.MsgID,
					Offset:    offset,
					Payload:   entry.Payload,
					Timestamp: entry.Timestamp,
				}
				messages = append(messages, msg)
				found = true
				break
			}
		}

		if !found {
			// If we can't find a message, stop here
			break
		}
	}

	// Advance read position by the number of messages we actually read
	q.readMsgID += uint64(len(messages))

	// Update metadata
	if err := q.metadata.SetReadMsgID(q.readMsgID); err != nil {
		return messages, fmt.Errorf("failed to update metadata: %w", err)
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages could be read")
	}

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
