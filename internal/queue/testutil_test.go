package queue

import (
	"fmt"
	"testing"
	"time"
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

// assertError checks that an error occurred and optionally contains expected text.
func assertError(t *testing.T, err error, wantContains string) {
	t.Helper()

	if err == nil {
		t.Errorf("expected error containing %q, got nil", wantContains)
		return
	}

	if wantContains != "" && !contains(err.Error(), wantContains) {
		t.Errorf("error = %q, want to contain %q", err.Error(), wantContains)
	}
}

// assertNoError fails the test if err is not nil.
func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// contains checks if s contains substr (simple helper to avoid importing strings in tests).
func contains(s, substr string) bool {
	return len(substr) == 0 || len(s) >= len(substr) && (s == substr || len(s) > len(substr) && indexOfString(s, substr) >= 0)
}

func indexOfString(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// waitFor polls a condition function until it returns true or timeout is reached.
// This eliminates flaky time.Sleep() calls in tests.
func waitFor(t *testing.T, condition func() bool, timeout time.Duration, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}

		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				t.Fatalf("timeout waiting for: %s", msg)
			}
		}
	}
}

// waitForStats waits for queue stats to match expected values.
func waitForStats(t *testing.T, q *Queue, total, pending uint64, timeout time.Duration) {
	t.Helper()

	waitFor(t, func() bool {
		stats := q.Stats()
		return stats.TotalMessages == total && stats.PendingMessages == pending
	}, timeout, fmt.Sprintf("stats to match total=%d pending=%d", total, pending))
}
