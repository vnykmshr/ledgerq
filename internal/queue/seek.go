// Package queue provides seek operations for queue navigation.
// This file contains methods for seeking to specific positions in the queue.
package queue

import (
	"fmt"
)

// SeekToMessageID sets the read position to a specific message ID.
// Subsequent Dequeue() calls will start reading from this message.
// This is useful for replay scenarios or skipping messages.
//
// The message ID must exist in the queue (between first and next message ID).
// After seeking, the read position is persisted to metadata.
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
