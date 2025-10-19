package queue

import (
	"testing"
	"time"

	"github.com/vnykmshr/ledgerq/internal/format"
)

// TestQueue_Priority_BasicOrdering tests that messages are dequeued in priority order
func TestQueue_Priority_BasicOrdering(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue messages in mixed priority order
	messages := []struct {
		priority uint8
		data     string
	}{
		{format.PriorityLow, "low1"},
		{format.PriorityHigh, "high1"},
		{format.PriorityMedium, "medium1"},
		{format.PriorityLow, "low2"},
		{format.PriorityHigh, "high2"},
		{format.PriorityMedium, "medium2"},
	}

	for _, msg := range messages {
		if _, err := q.EnqueueWithPriority([]byte(msg.data), msg.priority); err != nil {
			t.Fatalf("failed to enqueue %s: %v", msg.data, err)
		}
	}

	// Dequeue and verify priority order: all High, then all Medium, then all Low
	expectedOrder := []string{"high1", "high2", "medium1", "medium2", "low1", "low2"}

	for i, expected := range expectedOrder {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if string(msg.Payload) != expected {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
		}
	}
}

// TestQueue_Priority_FIFOWithinPriority tests that FIFO order is maintained within each priority
func TestQueue_Priority_FIFOWithinPriority(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue multiple high-priority messages
	for i := 1; i <= 5; i++ {
		data := []byte{byte(i)}
		if _, err := q.EnqueueWithPriority(data, format.PriorityHigh); err != nil {
			t.Fatalf("failed to enqueue message %d: %v", i, err)
		}
	}

	// Dequeue and verify FIFO order
	for i := 1; i <= 5; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if msg.Payload[0] != byte(i) {
			t.Errorf("message %d: got %d, want %d", i, msg.Payload[0], i)
		}
	}
}

// TestQueue_Priority_StarvationPrevention tests that low-priority messages are promoted after starvation window
func TestQueue_Priority_StarvationPrevention(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true
	opts.PriorityStarvationWindow = 100 * time.Millisecond // Short window for testing

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue a low-priority message
	if _, err := q.EnqueueWithPriority([]byte("low-starved"), format.PriorityLow); err != nil {
		t.Fatalf("failed to enqueue low-priority message: %v", err)
	}

	// Wait for starvation window to pass
	time.Sleep(150 * time.Millisecond)

	// Enqueue high-priority messages
	if _, err := q.EnqueueWithPriority([]byte("high1"), format.PriorityHigh); err != nil {
		t.Fatalf("failed to enqueue high-priority message: %v", err)
	}
	if _, err := q.EnqueueWithPriority([]byte("high2"), format.PriorityHigh); err != nil {
		t.Fatalf("failed to enqueue high-priority message: %v", err)
	}

	// First dequeue should return the starved low-priority message
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "low-starved" {
		t.Errorf("expected starved low-priority message, got %q", string(msg.Payload))
	}

	// Next dequeues should return high-priority messages
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high1" {
		t.Errorf("expected high1, got %q", string(msg.Payload))
	}

	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high2" {
		t.Errorf("expected high2, got %q", string(msg.Payload))
	}
}

// TestQueue_Priority_WithTTL tests priority queue with TTL expiration
func TestQueue_Priority_WithTTL(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue high-priority message with short TTL
	if _, err := q.EnqueueWithAllOptions([]byte("high-expired"), format.PriorityHigh, 50*time.Millisecond, nil); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Enqueue low-priority message without TTL
	if _, err := q.EnqueueWithPriority([]byte("low-valid"), format.PriorityLow); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Wait for high-priority message to expire
	time.Sleep(100 * time.Millisecond)

	// Dequeue should skip expired high-priority and return low-priority
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "low-valid" {
		t.Errorf("expected low-valid, got %q", string(msg.Payload))
	}
}

// TestQueue_Priority_WithHeaders tests priority queue with headers
func TestQueue_Priority_WithHeaders(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	headers := map[string]string{
		"content-type": "application/json",
		"trace-id":     "abc-123",
	}

	// Enqueue high-priority message with headers
	if _, err := q.EnqueueWithAllOptions([]byte("high-data"), format.PriorityHigh, 0, headers); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Enqueue low-priority message
	if _, err := q.EnqueueWithPriority([]byte("low-data"), format.PriorityLow); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue should return high-priority message with headers
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high-data" {
		t.Errorf("expected high-data, got %q", string(msg.Payload))
	}
	if msg.Headers["content-type"] != "application/json" {
		t.Errorf("expected content-type header, got %q", msg.Headers["content-type"])
	}
	if msg.Headers["trace-id"] != "abc-123" {
		t.Errorf("expected trace-id header, got %q", msg.Headers["trace-id"])
	}
}

// TestQueue_Priority_FIFOMode tests that FIFO mode still works when EnablePriorities is false
func TestQueue_Priority_FIFOMode(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = false // FIFO mode

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue messages with different priorities (should be ignored in FIFO mode)
	messages := []struct {
		priority uint8
		data     string
	}{
		{format.PriorityLow, "first"},
		{format.PriorityHigh, "second"},
		{format.PriorityMedium, "third"},
	}

	for _, msg := range messages {
		if _, err := q.EnqueueWithPriority([]byte(msg.data), msg.priority); err != nil {
			t.Fatalf("failed to enqueue %s: %v", msg.data, err)
		}
	}

	// Dequeue should return in FIFO order (insertion order)
	expectedOrder := []string{"first", "second", "third"}

	for i, expected := range expectedOrder {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if string(msg.Payload) != expected {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
		}
	}
}

// TestQueue_Priority_Batch tests that batch enqueue works with priority mode enabled
func TestQueue_Priority_Batch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// EnqueueBatch doesn't support priorities yet - all messages get default (PriorityLow)
	// This test verifies batch enqueue works in priority mode
	batch := [][]byte{
		[]byte("batch1"),
		[]byte("batch2"),
		[]byte("batch3"),
	}

	if _, err := q.EnqueueBatch(batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Dequeue and verify FIFO order within same priority
	expectedOrder := []string{"batch1", "batch2", "batch3"}

	for i, expected := range expectedOrder {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if string(msg.Payload) != expected {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
		}
	}
}

// TestQueue_Priority_Empty tests dequeue from empty priority queue
func TestQueue_Priority_Empty(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Dequeue from empty queue should return error
	_, err = q.Dequeue()
	if err == nil {
		t.Error("expected error when dequeueing from empty queue")
	}
}

// TestQueue_Priority_Persistence tests that priority index is rebuilt correctly after restart
func TestQueue_Priority_Persistence(t *testing.T) {
	dir := t.TempDir()

	// First queue: enqueue messages
	{
		opts := DefaultOptions(dir)
		opts.EnablePriorities = true

		q, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}

		// Enqueue messages with different priorities
		if _, err := q.EnqueueWithPriority([]byte("low1"), format.PriorityLow); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
		if _, err := q.EnqueueWithPriority([]byte("high1"), format.PriorityHigh); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
		if _, err := q.EnqueueWithPriority([]byte("medium1"), format.PriorityMedium); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		q.Close()
	}

	// Second queue: reopen and verify priority order is preserved
	{
		opts := DefaultOptions(dir)
		opts.EnablePriorities = true

		q, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("failed to reopen queue: %v", err)
		}
		defer q.Close()

		// Dequeue and verify priority order is preserved after restart
		expectedOrder := []string{"high1", "medium1", "low1"}

		for i, expected := range expectedOrder {
			msg, err := q.Dequeue()
			if err != nil {
				t.Fatalf("failed to dequeue message %d: %v", i, err)
			}
			if string(msg.Payload) != expected {
				t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
			}
		}
	}
}

// TestQueue_Priority_MixedOperations tests complex scenario with interleaved enqueue/dequeue
func TestQueue_Priority_MixedOperations(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue low-priority
	if _, err := q.EnqueueWithPriority([]byte("low1"), format.PriorityLow); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Enqueue high-priority
	if _, err := q.EnqueueWithPriority([]byte("high1"), format.PriorityHigh); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue (should get high1)
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high1" {
		t.Errorf("expected high1, got %q", string(msg.Payload))
	}

	// Enqueue medium-priority
	if _, err := q.EnqueueWithPriority([]byte("medium1"), format.PriorityMedium); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Enqueue high-priority
	if _, err := q.EnqueueWithPriority([]byte("high2"), format.PriorityHigh); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue (should get high2)
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high2" {
		t.Errorf("expected high2, got %q", string(msg.Payload))
	}

	// Dequeue (should get medium1)
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "medium1" {
		t.Errorf("expected medium1, got %q", string(msg.Payload))
	}

	// Dequeue (should get low1)
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "low1" {
		t.Errorf("expected low1, got %q", string(msg.Payload))
	}
}

// TestQueue_BatchWithOptions_Priority tests batch enqueue with priorities
func TestQueue_BatchWithOptions_Priority(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue batch with mixed priorities
	messages := []BatchEnqueueOptions{
		{Payload: []byte("low1"), Priority: format.PriorityLow},
		{Payload: []byte("high1"), Priority: format.PriorityHigh},
		{Payload: []byte("medium1"), Priority: format.PriorityMedium},
		{Payload: []byte("low2"), Priority: format.PriorityLow},
		{Payload: []byte("high2"), Priority: format.PriorityHigh},
	}

	offsets, err := q.EnqueueBatchWithOptions(messages)
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	if len(offsets) != len(messages) {
		t.Fatalf("expected %d offsets, got %d", len(messages), len(offsets))
	}

	// Dequeue and verify priority order
	expectedOrder := []string{"high1", "high2", "medium1", "low1", "low2"}

	for i, expected := range expectedOrder {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if string(msg.Payload) != expected {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
		}
	}
}

// TestQueue_BatchWithOptions_TTL tests batch enqueue with TTL
func TestQueue_BatchWithOptions_TTL(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue batch with TTL
	messages := []BatchEnqueueOptions{
		{Payload: []byte("expires"), Priority: format.PriorityHigh, TTL: 50 * time.Millisecond},
		{Payload: []byte("persistent"), Priority: format.PriorityLow, TTL: 0},
	}

	_, err = q.EnqueueBatchWithOptions(messages)
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for first message to expire
	time.Sleep(100 * time.Millisecond)

	// Should get second message (first expired)
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "persistent" {
		t.Errorf("expected persistent, got %q", string(msg.Payload))
	}
}

// TestQueue_BatchWithOptions_Headers tests batch enqueue with headers
func TestQueue_BatchWithOptions_Headers(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue batch with headers
	messages := []BatchEnqueueOptions{
		{
			Payload:  []byte("msg1"),
			Priority: format.PriorityHigh,
			Headers: map[string]string{
				"source": "server1",
				"type":   "alert",
			},
		},
		{
			Payload:  []byte("msg2"),
			Priority: format.PriorityLow,
			Headers: map[string]string{
				"source": "server2",
			},
		},
	}

	_, err = q.EnqueueBatchWithOptions(messages)
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Dequeue high priority first
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "msg1" {
		t.Errorf("expected msg1, got %q", string(msg.Payload))
	}
	if msg.Headers["source"] != "server1" {
		t.Errorf("expected source=server1, got %q", msg.Headers["source"])
	}
	if msg.Headers["type"] != "alert" {
		t.Errorf("expected type=alert, got %q", msg.Headers["type"])
	}
}

// TestQueue_BatchWithOptions_AllFeatures tests batch with all features combined
func TestQueue_BatchWithOptions_AllFeatures(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue batch with all features
	messages := []BatchEnqueueOptions{
		{
			Payload:  []byte("low-with-headers"),
			Priority: format.PriorityLow,
			Headers:  map[string]string{"key": "value1"},
		},
		{
			Payload:  []byte("high-with-ttl"),
			Priority: format.PriorityHigh,
			TTL:      100 * time.Millisecond,
		},
		{
			Payload:  []byte("medium-all"),
			Priority: format.PriorityMedium,
			TTL:      5 * time.Second,
			Headers:  map[string]string{"key": "value2"},
		},
	}

	offsets, err := q.EnqueueBatchWithOptions(messages)
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	if len(offsets) != 3 {
		t.Fatalf("expected 3 offsets, got %d", len(offsets))
	}

	// Dequeue in priority order
	// High first
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high-with-ttl" {
		t.Errorf("expected high-with-ttl, got %q", string(msg.Payload))
	}
	if msg.Priority != format.PriorityHigh {
		t.Errorf("expected high priority, got %d", msg.Priority)
	}

	// Medium second
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "medium-all" {
		t.Errorf("expected medium-all, got %q", string(msg.Payload))
	}
	if msg.Headers["key"] != "value2" {
		t.Errorf("expected key=value2, got %q", msg.Headers["key"])
	}

	// Low last
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "low-with-headers" {
		t.Errorf("expected low-with-headers, got %q", string(msg.Payload))
	}
	if msg.Headers["key"] != "value1" {
		t.Errorf("expected key=value1, got %q", msg.Headers["key"])
	}
}

// TestQueue_BatchWithOptions_EmptyBatch tests empty batch error handling
func TestQueue_BatchWithOptions_EmptyBatch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	_, err = q.EnqueueBatchWithOptions([]BatchEnqueueOptions{})
	if err == nil {
		t.Error("expected error for empty batch")
	}
}

// TestQueue_BatchWithOptions_FIFOMode tests batch with priorities in FIFO mode
func TestQueue_BatchWithOptions_FIFOMode(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = false // FIFO mode

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue batch with priorities (should be ignored in FIFO mode)
	messages := []BatchEnqueueOptions{
		{Payload: []byte("first"), Priority: format.PriorityLow},
		{Payload: []byte("second"), Priority: format.PriorityHigh},
		{Payload: []byte("third"), Priority: format.PriorityMedium},
	}

	_, err = q.EnqueueBatchWithOptions(messages)
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Should dequeue in FIFO order (insertion order)
	expectedOrder := []string{"first", "second", "third"}

	for i, expected := range expectedOrder {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if string(msg.Payload) != expected {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
		}
	}
}

// TestQueue_BatchWithOptions_InvalidPriority tests validation of invalid priority values
func TestQueue_BatchWithOptions_InvalidPriority(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Try to enqueue with invalid priority (> 2)
	messages := []BatchEnqueueOptions{
		{Payload: []byte("valid"), Priority: format.PriorityHigh},
		{Payload: []byte("invalid"), Priority: 5}, // Invalid priority
	}

	_, err = q.EnqueueBatchWithOptions(messages)
	if err == nil {
		t.Error("expected error for invalid priority")
	}
	if err != nil && err.Error() != "message 1: invalid priority 5 (must be 0-2)" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestQueue_BatchWithOptions_EmptyPayload tests validation of empty payload
func TestQueue_BatchWithOptions_EmptyPayload(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Try to enqueue with empty payload
	messages := []BatchEnqueueOptions{
		{Payload: []byte("valid"), Priority: format.PriorityHigh},
		{Payload: []byte{}, Priority: format.PriorityLow}, // Empty payload
	}

	_, err = q.EnqueueBatchWithOptions(messages)
	if err == nil {
		t.Error("expected error for empty payload")
	}
	if err != nil && err.Error() != "message 1: payload cannot be empty" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestQueue_EnqueueWithPriority_InvalidPriority tests validation of invalid priority values
func TestQueue_EnqueueWithPriority_InvalidPriority(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Try to enqueue with invalid priority (> 2)
	_, err = q.EnqueueWithPriority([]byte("test"), 5)
	if err == nil {
		t.Error("expected error for invalid priority")
	}
	if err != nil && err.Error() != "invalid priority 5 (must be 0-2)" {
		t.Errorf("unexpected error message: %v", err)
	}

	// Verify valid priorities still work
	if _, err := q.EnqueueWithPriority([]byte("valid"), format.PriorityHigh); err != nil {
		t.Errorf("unexpected error for valid priority: %v", err)
	}
}

// TestQueue_EnqueueWithAllOptions_InvalidPriority tests validation of invalid priority values
func TestQueue_EnqueueWithAllOptions_InvalidPriority(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Try to enqueue with invalid priority (> 2)
	_, err = q.EnqueueWithAllOptions([]byte("test"), 10, 0, nil)
	if err == nil {
		t.Error("expected error for invalid priority")
	}
	if err != nil && err.Error() != "invalid priority 10 (must be 0-2)" {
		t.Errorf("unexpected error message: %v", err)
	}

	// Verify valid priorities still work
	headers := map[string]string{"key": "value"}
	if _, err := q.EnqueueWithAllOptions([]byte("valid"), format.PriorityMedium, 1*time.Second, headers); err != nil {
		t.Errorf("unexpected error for valid priority: %v", err)
	}
}
