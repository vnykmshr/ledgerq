// Package queue provides message dequeue operations.
// This file contains FIFO and priority-based dequeue functionality.
package queue

import (
	"fmt"
	"time"

	"github.com/vnykmshr/ledgerq/internal/format"
	"github.com/vnykmshr/ledgerq/internal/logging"
)

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
