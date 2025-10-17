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
}

// DefaultOptions returns sensible defaults for queue configuration.
func DefaultOptions(dir string) *Options {
	return &Options{
		SegmentOptions: segment.DefaultManagerOptions(dir),
		AutoSync:       false,
		SyncInterval:   1 * time.Second,
	}
}

// Queue is a persistent, disk-backed message queue.
type Queue struct {
	opts *Options

	mu sync.RWMutex

	// Segment manager for storage
	segments *segment.Manager

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

	// Create segment manager
	segments, err := segment.NewManager(opts.SegmentOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment manager: %w", err)
	}

	// Determine next message ID by scanning existing segments
	nextMsgID, readMsgID, err := recoverState(segments)
	if err != nil {
		_ = segments.Close()
		return nil, fmt.Errorf("failed to recover queue state: %w", err)
	}

	q := &Queue{
		opts:      opts,
		segments:  segments,
		nextMsgID: nextMsgID,
		readMsgID: readMsgID,
	}

	// Start periodic sync timer if configured
	if !opts.AutoSync && opts.SyncInterval > 0 {
		q.startSyncTimer()
	}

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

	// Create entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     q.nextMsgID,
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Increment message ID
	q.nextMsgID++

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

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

	// Single sync for the entire batch
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offsets, fmt.Errorf("failed to sync batch: %w", err)
		}
	}

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

	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages could be read")
	}

	return messages, nil
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

	// Close segments
	if q.segments != nil {
		if err := q.segments.Close(); err != nil {
			return err
		}
	}

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
