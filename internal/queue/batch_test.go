package queue

import (
	"fmt"
	"testing"
)

func TestEnqueueBatch(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue a batch of messages
	payloads := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
	}

	offsets, err := q.EnqueueBatch(payloads)
	if err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

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

	// Verify stats
	stats := q.Stats()
	if stats.TotalMessages != uint64(len(payloads)) {
		t.Errorf("TotalMessages = %d, want %d", stats.TotalMessages, len(payloads))
	}
}

func TestEnqueueBatch_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Try to enqueue empty batch
	_, err = q.EnqueueBatch([][]byte{})
	if err == nil {
		t.Error("EnqueueBatch() with empty batch should fail")
	}
}

func TestEnqueueBatch_Large(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue a large batch
	count := 100
	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		payloads[i] = []byte(fmt.Sprintf("message %d", i))
	}

	offsets, err := q.EnqueueBatch(payloads)
	if err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	if len(offsets) != count {
		t.Errorf("EnqueueBatch() returned %d offsets, want %d", len(offsets), count)
	}

	stats := q.Stats()
	if stats.TotalMessages != uint64(count) {
		t.Errorf("TotalMessages = %d, want %d", stats.TotalMessages, count)
	}
}

func TestDequeueBatch(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue some messages
	payloads := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
		[]byte("message 4"),
		[]byte("message 5"),
	}

	if _, err := q.EnqueueBatch(payloads); err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	// Dequeue a batch
	messages, err := q.DequeueBatch(3)
	if err != nil {
		t.Fatalf("DequeueBatch() error = %v", err)
	}

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
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue 3 messages
	payloads := [][]byte{
		[]byte("msg1"),
		[]byte("msg2"),
		[]byte("msg3"),
	}

	if _, err := q.EnqueueBatch(payloads); err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	// Try to dequeue 10 messages (only 3 available)
	messages, err := q.DequeueBatch(10)
	if err != nil {
		t.Fatalf("DequeueBatch() error = %v", err)
	}

	if len(messages) != 3 {
		t.Errorf("DequeueBatch(10) returned %d messages, want 3", len(messages))
	}
}

func TestDequeueBatch_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Try to dequeue from empty queue
	_, err = q.DequeueBatch(5)
	if err == nil {
		t.Error("DequeueBatch() on empty queue should fail")
	}
}

func TestDequeueBatch_InvalidSize(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Try to dequeue with invalid size
	_, err = q.DequeueBatch(0)
	if err == nil {
		t.Error("DequeueBatch(0) should fail")
	}

	_, err = q.DequeueBatch(-1)
	if err == nil {
		t.Error("DequeueBatch(-1) should fail")
	}
}

func TestBatch_MixedOperations(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue batch
	batch1 := [][]byte{
		[]byte("batch1-msg1"),
		[]byte("batch1-msg2"),
	}
	if _, err := q.EnqueueBatch(batch1); err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	// Enqueue single
	if _, err := q.Enqueue([]byte("single-msg")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// Enqueue another batch
	batch2 := [][]byte{
		[]byte("batch2-msg1"),
		[]byte("batch2-msg2"),
	}
	if _, err := q.EnqueueBatch(batch2); err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	// Should have 5 total messages
	stats := q.Stats()
	if stats.TotalMessages != 5 {
		t.Errorf("TotalMessages = %d, want 5", stats.TotalMessages)
	}

	// Dequeue batch of 3
	messages, err := q.DequeueBatch(3)
	if err != nil {
		t.Fatalf("DequeueBatch() error = %v", err)
	}

	if len(messages) != 3 {
		t.Errorf("DequeueBatch() returned %d messages, want 3", len(messages))
	}

	// Dequeue single
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

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
	q1, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() first error = %v", err)
	}

	payloads := [][]byte{
		[]byte("batch-msg1"),
		[]byte("batch-msg2"),
		[]byte("batch-msg3"),
	}

	if _, err := q1.EnqueueBatch(payloads); err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	if err := q1.Close(); err != nil {
		t.Fatalf("Close() first error = %v", err)
	}

	// Second session: verify persistence
	q2, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() second error = %v", err)
	}
	defer func() { _ = q2.Close() }()

	stats := q2.Stats()
	if stats.TotalMessages != 3 {
		t.Errorf("Recovered TotalMessages = %d, want 3", stats.TotalMessages)
	}

	// Dequeue batch and verify
	messages, err := q2.DequeueBatch(3)
	if err != nil {
		t.Fatalf("DequeueBatch() error = %v", err)
	}

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

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Try to enqueue batch after close
	_, err = q.EnqueueBatch([][]byte{[]byte("test")})
	if err == nil {
		t.Error("EnqueueBatch() after Close() should fail")
	}
}

func TestDequeueBatch_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Enqueue before close
	if _, err := q.EnqueueBatch([][]byte{[]byte("test")}); err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Try to dequeue batch after close
	_, err = q.DequeueBatch(1)
	if err == nil {
		t.Error("DequeueBatch() after Close() should fail")
	}
}
