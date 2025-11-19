package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStream_Basic(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue messages before streaming
	messages := []string{"msg1", "msg2", "msg3"}
	enqueueMessages(t, q, messages)

	// Stream messages with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var received []string
	var mu sync.Mutex

	handler := func(msg *Message) error {
		mu.Lock()
		received = append(received, string(msg.Payload))
		mu.Unlock()

		// Cancel after receiving all messages
		if len(received) == len(messages) {
			cancel()
		}
		return nil
	}

	err := q.Stream(ctx, handler)

	// Should return context.Canceled after we cancel
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Stream() error = %v, want context.Canceled", err)
	}

	// Verify all messages were received
	mu.Lock()
	defer mu.Unlock()

	if len(received) != len(messages) {
		t.Errorf("Received %d messages, want %d", len(received), len(messages))
	}

	for i, expected := range messages {
		if i >= len(received) {
			t.Errorf("Missing message %d: %s", i, expected)
			continue
		}
		if received[i] != expected {
			t.Errorf("Message %d = %s, want %s", i, received[i], expected)
		}
	}
}

// TestStream_ContextCancellation tests graceful shutdown on context cancellation
func TestStream_ContextCancellation(t *testing.T) {
	q := setupQueue(t, nil)
	enqueueN(t, q, 100)

	// Create context that we'll cancel after a few messages
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var count atomic.Int32

	handler := func(msg *Message) error {
		count.Add(1)
		// Cancel after 5 messages
		if count.Load() == 5 {
			cancel()
		}
		return nil
	}

	err := q.Stream(ctx, handler)

	// Should return context.Canceled
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Stream() error = %v, want context.Canceled", err)
	}

	// Should have received exactly 5 messages
	if count.Load() != 5 {
		t.Errorf("Received %d messages, want 5", count.Load())
	}
}

// TestStream_HandlerError tests that handler errors stop streaming
func TestStream_HandlerError(t *testing.T) {
	q := setupQueue(t, nil)
	enqueueN(t, q, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	testErr := errors.New("test error")
	var count atomic.Int32

	handler := func(msg *Message) error {
		count.Add(1)
		// Return error after 3 messages
		if count.Load() == 3 {
			return testErr
		}
		return nil
	}

	err := q.Stream(ctx, handler)

	// Should return the handler error
	if !errors.Is(err, testErr) {
		t.Errorf("Stream() error = %v, want test error", err)
	}

	// Should have received exactly 3 messages
	if count.Load() != 3 {
		t.Errorf("Received %d messages, want 3", count.Load())
	}
}

// TestStream_EmptyQueue tests streaming from an empty queue
func TestStream_EmptyQueue(t *testing.T) {
	q := setupQueue(t, nil)

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	handlerCalled := false
	handler := func(msg *Message) error {
		handlerCalled = true
		return nil
	}

	err := q.Stream(ctx, handler)

	// Should timeout with context.DeadlineExceeded
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stream() error = %v, want context.DeadlineExceeded", err)
	}

	// Handler should never be called for empty queue
	if handlerCalled {
		t.Error("Handler should not be called for empty queue")
	}
}

// TestStream_ConcurrentProducer tests streaming while messages are being produced
func TestStream_ConcurrentProducer(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	var receivedCount atomic.Int32
	totalToSend := 20

	// Use channel to signal when all messages received
	done := make(chan struct{})

	handler := func(msg *Message) error {
		if receivedCount.Add(1) >= int32(totalToSend) {
			close(done)
		}
		return nil
	}

	// Start streaming in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var streamErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = q.Stream(ctx, handler)
	}()

	// Give stream a moment to start
	time.Sleep(50 * time.Millisecond)

	// Produce messages while streaming
	for i := 0; i < totalToSend; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
		// Small delay between messages
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all messages to be received or timeout
	select {
	case <-done:
		cancel() // Success - cancel the stream
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatalf("Timeout waiting for messages: received %d, want %d", receivedCount.Load(), totalToSend)
	}

	// Wait for stream to finish
	wg.Wait()

	// Should complete with context.Canceled
	if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
		t.Errorf("Stream() error = %v, want context.Canceled", streamErr)
	}
}

// TestStream_NilHandler tests that nil handler returns error
func TestStream_NilHandler(t *testing.T) {
	q := setupQueue(t, nil)

	ctx := context.Background()

	err := q.Stream(ctx, nil)
	if err == nil {
		t.Error("Stream() with nil handler should fail")
	}
}

// TestStream_MessageOrder tests that streaming maintains FIFO order
func TestStream_MessageOrder(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages with sequential IDs
	numMessages := 50
	for i := 0; i < numMessages; i++ {
		payload := []byte{byte(i)}
		if _, err := q.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var received []byte
	var mu sync.Mutex

	handler := func(msg *Message) error {
		mu.Lock()
		received = append(received, msg.Payload[0])
		mu.Unlock()

		// Cancel after all messages
		if len(received) == numMessages {
			cancel()
		}
		return nil
	}

	err = q.Stream(ctx, handler)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Stream() error = %v", err)
	}

	// Verify FIFO order
	mu.Lock()
	defer mu.Unlock()

	if len(received) != numMessages {
		t.Fatalf("Received %d messages, want %d", len(received), numMessages)
	}

	for i := 0; i < numMessages; i++ {
		if received[i] != byte(i) {
			t.Errorf("Message %d out of order: got %d, want %d", i, received[i], i)
		}
	}
}

// TestStream_AfterClose tests streaming after queue is closed
func TestStream_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	q := setupQueue(t, DefaultOptions(tmpDir))
	enqueueN(t, q, 1)
	assertNoError(t, q.Close())

	ctx := context.Background()
	handler := func(msg *Message) error {
		return nil
	}

	err := q.Stream(ctx, handler)
	if err == nil {
		t.Error("Stream() after Close() should fail")
	}
}
