package queue

import (
	"testing"
	"time"
)

// TestEnqueueWithTTL tests basic TTL enqueue functionality
func TestEnqueueWithTTL(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue with 1 second TTL
	payload := []byte("test message with TTL")
	offset, err := q.EnqueueWithTTL(payload, 1*time.Second)
	assertNoError(t, err)

	if offset == 0 {
		t.Error("EnqueueWithTTL() offset = 0, want > 0")
	}

	// Dequeue immediately (should succeed)
	msg, err := q.Dequeue()
	assertNoError(t, err)

	if string(msg.Payload) != string(payload) {
		t.Errorf("Payload = %s, want %s", msg.Payload, payload)
	}

	if msg.ExpiresAt == 0 {
		t.Error("ExpiresAt = 0, want > 0 for TTL message")
	}

	// Verify ExpiresAt is in the future
	now := time.Now().UnixNano()
	if msg.ExpiresAt <= now {
		t.Error("ExpiresAt should be in the future")
	}
}

// TestTTL_MessageExpiration tests that expired messages are skipped
func TestTTL_MessageExpiration(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue message with very short TTL (100ms)
	_, err := q.EnqueueWithTTL([]byte("expires soon"), 100*time.Millisecond)
	assertNoError(t, err)

	// Enqueue normal message
	_, err = q.Enqueue([]byte("normal message"))
	assertNoError(t, err)

	// Wait for first message to expire
	time.Sleep(150 * time.Millisecond)

	// Dequeue should skip expired message and return second message
	msg, err := q.Dequeue()
	assertNoError(t, err)

	if string(msg.Payload) != "normal message" {
		t.Errorf("Got %s, expected normal message (expired message should be skipped)", msg.Payload)
	}

	// Verify we're at message ID 3 (skipped 1, consumed 2)
	stats := q.Stats()
	if stats.ReadMessageID != 3 {
		t.Errorf("ReadMessageID = %d, want 3", stats.ReadMessageID)
	}
}

// TestTTL_MultipleExpiredMessages tests skipping multiple expired messages
func TestTTL_MultipleExpiredMessages(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue 3 messages with short TTL
	for i := 0; i < 3; i++ {
		_, err := q.EnqueueWithTTL([]byte("expires"), 50*time.Millisecond)
		if err != nil {
			t.Fatalf("EnqueueWithTTL() error = %v", err)
		}
	}

	// Enqueue valid message
	_, err := q.Enqueue([]byte("valid"))
	assertNoError(t, err)

	// Wait for TTL messages to expire
	time.Sleep(100 * time.Millisecond)

	// Dequeue should skip all 3 expired and return the valid one
	msg, err := q.Dequeue()
	assertNoError(t, err)

	if string(msg.Payload) != "valid" {
		t.Errorf("Got %s, expected valid", msg.Payload)
	}

	// Read position should be at 5 (skipped 1,2,3, consumed 4)
	stats := q.Stats()
	if stats.ReadMessageID != 5 {
		t.Errorf("ReadMessageID = %d, want 5", stats.ReadMessageID)
	}
}

// TestTTL_BatchDequeue tests TTL with batch operations
func TestTTL_BatchDequeue(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue: expired, valid, expired, valid, expired
	_, err := q.EnqueueWithTTL([]byte("expired1"), 50*time.Millisecond)
	assertNoError(t, err)

	_, err = q.Enqueue([]byte("valid1"))
	assertNoError(t, err)

	_, err = q.EnqueueWithTTL([]byte("expired2"), 50*time.Millisecond)
	assertNoError(t, err)

	_, err = q.Enqueue([]byte("valid2"))
	assertNoError(t, err)

	_, err = q.EnqueueWithTTL([]byte("expired3"), 50*time.Millisecond)
	assertNoError(t, err)

	// Wait for TTL messages to expire
	time.Sleep(100 * time.Millisecond)

	// Batch dequeue should skip expired and return only valid ones
	messages, err := q.DequeueBatch(10)
	assertNoError(t, err)

	// Should get 2 valid messages
	if len(messages) != 2 {
		t.Fatalf("Got %d messages, want 2", len(messages))
	}

	if string(messages[0].Payload) != "valid1" {
		t.Errorf("Message 0 = %s, want valid1", messages[0].Payload)
	}

	if string(messages[1].Payload) != "valid2" {
		t.Errorf("Message 1 = %s, want valid2", messages[1].Payload)
	}
}

// TestTTL_InvalidDuration tests that invalid TTL values are rejected
func TestTTL_InvalidDuration(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
	}{
		{"zero TTL", 0},
		{"negative TTL", -1 * time.Second},
		{"negative nanosecond", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := setupQueue(t, nil)
			_, err := q.EnqueueWithTTL([]byte("test"), tt.ttl)
			if err == nil {
				t.Errorf("EnqueueWithTTL() with %s should fail", tt.name)
			}
		})
	}
}

// TestTTL_Persistence tests that TTL is persisted correctly
func TestTTL_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: enqueue with TTL
	q1 := setupQueue(t, DefaultOptions(tmpDir))

	_, err := q1.EnqueueWithTTL([]byte("persisted"), 5*time.Second)
	assertNoError(t, err)

	assertNoError(t, q1.Close())

	// Second session: verify TTL is preserved
	q2 := setupQueue(t, DefaultOptions(tmpDir))

	msg, err := q2.Dequeue()
	assertNoError(t, err)

	if msg.ExpiresAt == 0 {
		t.Error("ExpiresAt = 0 after persistence, want > 0")
	}

	if string(msg.Payload) != "persisted" {
		t.Errorf("Payload = %s, want persisted", msg.Payload)
	}
}

// TestTTL_LongDuration tests messages with long TTL
func TestTTL_LongDuration(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue with 1 hour TTL
	_, err := q.EnqueueWithTTL([]byte("long lived"), 1*time.Hour)
	assertNoError(t, err)

	// Should be immediately dequeueable
	msg, err := q.Dequeue()
	assertNoError(t, err)

	if string(msg.Payload) != "long lived" {
		t.Errorf("Payload = %s, want long lived", msg.Payload)
	}

	// Verify ExpiresAt is about 1 hour in the future
	now := time.Now().UnixNano()
	expectedExpiration := now + (1 * time.Hour).Nanoseconds()
	diff := msg.ExpiresAt - expectedExpiration

	// Allow 1 second tolerance
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Second.Nanoseconds() {
		t.Errorf("ExpiresAt diff = %d ns, want < 1 second", diff)
	}
}

// TestTTL_MixedMessages tests mix of TTL and non-TTL messages
func TestTTL_MixedMessages(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue mixed messages
	messages := []struct {
		payload string
		ttl     *time.Duration
	}{
		{"normal1", nil},
		{"ttl1", durationPtr(5 * time.Second)},
		{"normal2", nil},
		{"ttl2", durationPtr(10 * time.Second)},
		{"normal3", nil},
	}

	for _, m := range messages {
		var err error
		if m.ttl != nil {
			_, err = q.EnqueueWithTTL([]byte(m.payload), *m.ttl)
		} else {
			_, err = q.Enqueue([]byte(m.payload))
		}
		if err != nil {
			t.Fatalf("Enqueue error = %v", err)
		}
	}

	// Dequeue all and verify
	for i, expected := range messages {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue() %d error = %v", i, err)
		}

		if string(msg.Payload) != expected.payload {
			t.Errorf("Message %d = %s, want %s", i, msg.Payload, expected.payload)
		}

		if expected.ttl != nil {
			if msg.ExpiresAt == 0 {
				t.Errorf("Message %d has TTL but ExpiresAt = 0", i)
			}
		} else {
			if msg.ExpiresAt != 0 {
				t.Errorf("Message %d has no TTL but ExpiresAt = %d", i, msg.ExpiresAt)
			}
		}
	}
}

// TestTTL_AllExpired tests when all messages are expired
func TestTTL_AllExpired(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue only expired messages
	for i := 0; i < 5; i++ {
		_, err := q.EnqueueWithTTL([]byte("expired"), 50*time.Millisecond)
		if err != nil {
			t.Fatalf("EnqueueWithTTL() error = %v", err)
		}
	}

	// Wait for all to expire
	time.Sleep(100 * time.Millisecond)

	// Dequeue should fail with "no messages available"
	_, err := q.Dequeue()
	if err == nil {
		t.Error("Dequeue() should fail when all messages are expired")
	}

	// Verify read position advanced past all expired messages
	stats := q.Stats()
	if stats.ReadMessageID != 6 {
		t.Errorf("ReadMessageID = %d, want 6 (after skipping all 5 expired)", stats.ReadMessageID)
	}
}

// Helper function to create duration pointer
func durationPtr(d time.Duration) *time.Duration {
	return &d
}
