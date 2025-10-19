package queue

import (
	"fmt"
	"testing"
)

func TestEnqueueBatch(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue a batch of messages
	payloads := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
	}

	offsets, err := q.EnqueueBatch(payloads)
	assertNoError(t, err)

	if len(offsets) != len(payloads) {
		t.Errorf("EnqueueBatch() returned %d offsets, want %d", len(offsets), len(payloads))
	}

	// Verify all offsets are non-zero and increasing
	for i, offset := range offsets {
		if offset == 0 {
			t.Errorf("EnqueueBatch() offset[%d] = 0, want > 0", i)
		}

		if i > 0 && offset <= offsets[i-1] {
			t.Errorf("EnqueueBatch() offset[%d] = %d, should be > %d", i, offset, offsets[i-1])
		}
	}

	assertStats(t, q, 3, 3)
}

func TestEnqueueBatch_Empty(t *testing.T) {
	q := setupQueue(t, nil)

	// Try to enqueue empty batch
	_, err := q.EnqueueBatch([][]byte{})
	if err == nil {
		t.Error("EnqueueBatch() with empty batch should fail")
	}
}

func TestEnqueueBatch_Large(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue a large batch
	count := 100
	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		payloads[i] = []byte(fmt.Sprintf("message %d", i))
	}

	offsets, err := q.EnqueueBatch(payloads)
	assertNoError(t, err)

	if len(offsets) != count {
		t.Errorf("EnqueueBatch() returned %d offsets, want %d", len(offsets), count)
	}

	assertStats(t, q, 100, 100)
}

func TestDequeueBatch(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue some messages
	payloads := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
		[]byte("message 4"),
		[]byte("message 5"),
	}

	_, err := q.EnqueueBatch(payloads)
	assertNoError(t, err)

	// Dequeue a batch
	messages, err := q.DequeueBatch(3)
	assertNoError(t, err)

	if len(messages) != 3 {
		t.Errorf("DequeueBatch() returned %d messages, want 3", len(messages))
	}

	// Verify message order and content
	for i, msg := range messages {
		expectedID := uint64(i + 1)
		if msg.ID != expectedID {
			t.Errorf("Message[%d] ID = %d, want %d", i, msg.ID, expectedID)
		}

		expected := fmt.Sprintf("message %d", i+1)
		if string(msg.Payload) != expected {
			t.Errorf("Message[%d] Payload = %s, want %s", i, msg.Payload, expected)
		}
	}

	// Stats should reflect consumption
	stats := q.Stats()
	if stats.PendingMessages != 2 {
		t.Errorf("PendingMessages = %d, want 2", stats.PendingMessages)
	}
}

func TestDequeueBatch_MoreThanAvailable(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue 3 messages
	payloads := [][]byte{
		[]byte("msg1"),
		[]byte("msg2"),
		[]byte("msg3"),
	}

	_, err := q.EnqueueBatch(payloads)
	assertNoError(t, err)

	// Try to dequeue 10 messages (only 3 available)
	messages, err := q.DequeueBatch(10)
	assertNoError(t, err)

	if len(messages) != 3 {
		t.Errorf("DequeueBatch(10) returned %d messages, want 3", len(messages))
	}
}

func TestDequeueBatch_Empty(t *testing.T) {
	q := setupQueue(t, nil)

	// Try to dequeue from empty queue
	_, err := q.DequeueBatch(5)
	if err == nil {
		t.Error("DequeueBatch() on empty queue should fail")
	}
}

func TestDequeueBatch_InvalidSize(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"zero size", 0},
		{"negative size", -1},
		{"large negative", -100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := setupQueue(t, nil)
			_, err := q.DequeueBatch(tt.size)
			if err == nil {
				t.Errorf("DequeueBatch(%d) should fail", tt.size)
			}
		})
	}
}

func TestBatch_MixedOperations(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue batch
	batch1 := [][]byte{
		[]byte("batch1-msg1"),
		[]byte("batch1-msg2"),
	}
	_, err := q.EnqueueBatch(batch1)
	assertNoError(t, err)

	// Enqueue single
	_, err = q.Enqueue([]byte("single-msg"))
	assertNoError(t, err)

	// Enqueue another batch
	batch2 := [][]byte{
		[]byte("batch2-msg1"),
		[]byte("batch2-msg2"),
	}
	_, err = q.EnqueueBatch(batch2)
	assertNoError(t, err)

	// Should have 5 total messages
	stats := q.Stats()
	if stats.TotalMessages != 5 {
		t.Errorf("TotalMessages = %d, want 5", stats.TotalMessages)
	}

	// Dequeue batch of 3
	messages, err := q.DequeueBatch(3)
	assertNoError(t, err)

	if len(messages) != 3 {
		t.Errorf("DequeueBatch() returned %d messages, want 3", len(messages))
	}

	// Dequeue single
	msg, err := q.Dequeue()
	assertNoError(t, err)

	if string(msg.Payload) != "batch2-msg1" {
		t.Errorf("Dequeue() payload = %s, want batch2-msg1", msg.Payload)
	}

	// One message remaining
	stats = q.Stats()
	if stats.PendingMessages != 1 {
		t.Errorf("PendingMessages = %d, want 1", stats.PendingMessages)
	}
}

func TestBatch_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: enqueue batch
	q1 := setupQueue(t, DefaultOptions(tmpDir))

	payloads := [][]byte{
		[]byte("batch-msg1"),
		[]byte("batch-msg2"),
		[]byte("batch-msg3"),
	}

	_, err := q1.EnqueueBatch(payloads)
	assertNoError(t, err)
	assertNoError(t, q1.Close())

	// Second session: verify persistence
	q2 := setupQueue(t, DefaultOptions(tmpDir))

	stats := q2.Stats()
	if stats.TotalMessages != 3 {
		t.Errorf("Recovered TotalMessages = %d, want 3", stats.TotalMessages)
	}

	// Dequeue batch and verify
	messages, err := q2.DequeueBatch(3)
	assertNoError(t, err)

	if len(messages) != 3 {
		t.Errorf("DequeueBatch() returned %d messages, want 3", len(messages))
	}

	for i, msg := range messages {
		expected := fmt.Sprintf("batch-msg%d", i+1)
		if string(msg.Payload) != expected {
			t.Errorf("Message[%d] Payload = %s, want %s", i, msg.Payload, expected)
		}
	}
}

func TestEnqueueBatch_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	q := setupQueue(t, DefaultOptions(tmpDir))
	assertNoError(t, q.Close())

	// Try to enqueue batch after close
	_, err := q.EnqueueBatch([][]byte{[]byte("test")})
	if err == nil {
		t.Error("EnqueueBatch() after Close() should fail")
	}
}

func TestDequeueBatch_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	q := setupQueue(t, DefaultOptions(tmpDir))

	// Enqueue before close
	_, err := q.EnqueueBatch([][]byte{[]byte("test")})
	assertNoError(t, err)
	assertNoError(t, q.Close())

	// Try to dequeue batch after close
	_, err = q.DequeueBatch(1)
	if err == nil {
		t.Error("DequeueBatch() after Close() should fail")
	}
}
