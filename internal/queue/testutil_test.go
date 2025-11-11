package queue

import (
	"fmt"
	"testing"
)

// setupQueue creates a test queue with optional options.
// The queue is automatically closed when the test completes.
func setupQueue(t *testing.T, opts *Options) *Queue {
	t.Helper()

	tmpDir := t.TempDir()
	if opts == nil {
		opts = DefaultOptions(tmpDir)
	} else if opts.SegmentOptions != nil && opts.SegmentOptions.Directory == "" {
		opts.SegmentOptions.Directory = tmpDir
	}

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	t.Cleanup(func() { _ = q.Close() })

	return q
}

// enqueueN enqueues n messages and returns their offsets.
// Messages are in the format "msg-0", "msg-1", etc.
func enqueueN(t *testing.T, q *Queue, n int) []uint64 {
	t.Helper()

	offsets := make([]uint64, n)
	for i := 0; i < n; i++ {
		offset, err := q.Enqueue([]byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
		offsets[i] = offset
	}

	return offsets
}

// enqueueMessages enqueues specific messages and returns their offsets.
func enqueueMessages(t *testing.T, q *Queue, messages []string) []uint64 {
	t.Helper()

	offsets := make([]uint64, len(messages))
	for i, msg := range messages {
		offset, err := q.Enqueue([]byte(msg))
		if err != nil {
			t.Fatalf("enqueue message %d (%s) failed: %v", i, msg, err)
		}
		offsets[i] = offset
	}

	return offsets
}

// dequeueN dequeues n messages and returns them.
// Fails the test if fewer messages are available.
func dequeueN(t *testing.T, q *Queue, n int) []*Message {
	t.Helper()

	messages := make([]*Message, n)
	for i := 0; i < n; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("dequeue %d failed: %v", i, err)
		}
		messages[i] = msg
	}

	return messages
}

// assertStats validates queue statistics.
func assertStats(t *testing.T, q *Queue, total, pending uint64) {
	t.Helper()

	stats := q.Stats()
	if stats.TotalMessages != total {
		t.Errorf("TotalMessages = %d, want %d", stats.TotalMessages, total)
	}
	if stats.PendingMessages != pending {
		t.Errorf("PendingMessages = %d, want %d", stats.PendingMessages, pending)
	}
}

// assertStatsDetailed validates detailed queue statistics.
func assertStatsDetailed(t *testing.T, q *Queue, total, pending, nextID, readID uint64) {
	t.Helper()

	stats := q.Stats()
	if stats.TotalMessages != total {
		t.Errorf("TotalMessages = %d, want %d", stats.TotalMessages, total)
	}
	if stats.PendingMessages != pending {
		t.Errorf("PendingMessages = %d, want %d", stats.PendingMessages, pending)
	}
	if stats.NextMessageID != nextID {
		t.Errorf("NextMessageID = %d, want %d", stats.NextMessageID, nextID)
	}
	if stats.ReadMessageID != readID {
		t.Errorf("ReadMessageID = %d, want %d", stats.ReadMessageID, readID)
	}
}

// assertPayloads verifies that dequeued messages match expected payloads in order.
func assertPayloads(t *testing.T, messages []*Message, expected []string) {
	t.Helper()

	if len(messages) != len(expected) {
		t.Fatalf("got %d messages, want %d", len(messages), len(expected))
	}

	for i, msg := range messages {
		if string(msg.Payload) != expected[i] {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected[i])
		}
	}
}

// assertNoError fails the test if err is not nil.
func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
