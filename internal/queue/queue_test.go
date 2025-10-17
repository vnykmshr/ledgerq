package queue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	if q.IsClosed() {
		t.Error("IsClosed() = true, want false for new queue")
	}

	// Check initial state
	stats := q.Stats()
	if stats.NextMessageID != 1 {
		t.Errorf("NextMessageID = %d, want 1", stats.NextMessageID)
	}

	if stats.ReadMessageID != 1 {
		t.Errorf("ReadMessageID = %d, want 1", stats.ReadMessageID)
	}

	if stats.TotalMessages != 0 {
		t.Errorf("TotalMessages = %d, want 0", stats.TotalMessages)
	}
}

func TestOpen_WithExistingData(t *testing.T) {
	tmpDir := t.TempDir()

	// Create queue and add some messages
	q1, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() first error = %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := q1.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	if err := q1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen and verify state is recovered
	q2, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() second error = %v", err)
	}
	defer func() { _ = q2.Close() }()

	stats := q2.Stats()
	if stats.NextMessageID != 6 {
		t.Errorf("Recovered NextMessageID = %d, want 6", stats.NextMessageID)
	}

	if stats.TotalMessages != 5 {
		t.Errorf("Recovered TotalMessages = %d, want 5", stats.TotalMessages)
	}
}

func TestEnqueue(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	payload := []byte("test message")
	offset, err := q.Enqueue(payload)
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if offset == 0 {
		t.Error("Enqueue() offset = 0, want > 0")
	}

	stats := q.Stats()
	if stats.TotalMessages != 1 {
		t.Errorf("TotalMessages = %d, want 1", stats.TotalMessages)
	}

	if stats.NextMessageID != 2 {
		t.Errorf("NextMessageID = %d, want 2", stats.NextMessageID)
	}
}

func TestEnqueue_Multiple(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	count := 10
	for i := 0; i < count; i++ {
		payload := []byte("test message")
		if _, err := q.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue() %d error = %v", i, err)
		}
	}

	stats := q.Stats()
	if stats.TotalMessages != uint64(count) {
		t.Errorf("TotalMessages = %d, want %d", stats.TotalMessages, count)
	}

	if stats.PendingMessages != uint64(count) {
		t.Errorf("PendingMessages = %d, want %d", stats.PendingMessages, count)
	}
}

func TestDequeue(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue a message
	payload := []byte("test message")
	_, err = q.Enqueue(payload)
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// Dequeue it
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if msg.ID != 1 {
		t.Errorf("Message ID = %d, want 1", msg.ID)
	}

	if string(msg.Payload) != string(payload) {
		t.Errorf("Message Payload = %s, want %s", msg.Payload, payload)
	}

	if msg.Timestamp == 0 {
		t.Error("Message Timestamp = 0, want > 0")
	}

	// Stats should reflect consumption
	stats := q.Stats()
	if stats.PendingMessages != 0 {
		t.Errorf("PendingMessages = %d, want 0", stats.PendingMessages)
	}

	if stats.ReadMessageID != 2 {
		t.Errorf("ReadMessageID = %d, want 2", stats.ReadMessageID)
	}
}

func TestDequeue_FIFO(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue multiple messages
	messages := []string{"first", "second", "third"}
	for _, payload := range messages {
		if _, err := q.Enqueue([]byte(payload)); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Dequeue and verify FIFO order
	for i, expected := range messages {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue() %d error = %v", i, err)
		}

		if string(msg.Payload) != expected {
			t.Errorf("Dequeue() %d payload = %s, want %s", i, msg.Payload, expected)
		}

		if msg.ID != uint64(i+1) {
			t.Errorf("Dequeue() %d ID = %d, want %d", i, msg.ID, i+1)
		}
	}
}

func TestDequeue_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Try to dequeue from empty queue
	_, err = q.Dequeue()
	if err == nil {
		t.Error("Dequeue() on empty queue should fail")
	}
}

func TestDequeue_AfterConsumeAll(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue and dequeue one message
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if _, err := q.Dequeue(); err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	// Try to dequeue again (should fail)
	_, err = q.Dequeue()
	if err == nil {
		t.Error("Dequeue() after consuming all messages should fail")
	}
}

func TestSync(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = false // Disable auto-sync

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue without auto-sync
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// Manual sync
	if err := q.Sync(); err != nil {
		t.Errorf("Sync() error = %v", err)
	}
}

func TestAutoSync(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = true

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue with auto-sync
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// Data should be synced automatically
	// (We can't easily verify this, but the call should succeed)
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Enqueue a message
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if !q.IsClosed() {
		t.Error("IsClosed() = false, want true after Close()")
	}

	// Double close should be safe
	if err := q.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func TestEnqueue_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Try to enqueue after close
	_, err = q.Enqueue([]byte("test"))
	if err == nil {
		t.Error("Enqueue() after Close() should fail")
	}
}

func TestDequeue_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Enqueue before close
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Try to dequeue after close
	_, err = q.Dequeue()
	if err == nil {
		t.Error("Dequeue() after Close() should fail")
	}
}

func TestStats(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Initial stats
	stats := q.Stats()
	if stats.TotalMessages != 0 {
		t.Errorf("Initial TotalMessages = %d, want 0", stats.TotalMessages)
	}

	if stats.PendingMessages != 0 {
		t.Errorf("Initial PendingMessages = %d, want 0", stats.PendingMessages)
	}

	// Enqueue some messages
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	stats = q.Stats()
	if stats.TotalMessages != 5 {
		t.Errorf("TotalMessages = %d, want 5", stats.TotalMessages)
	}

	if stats.PendingMessages != 5 {
		t.Errorf("PendingMessages = %d, want 5", stats.PendingMessages)
	}

	// Dequeue some messages
	for i := 0; i < 2; i++ {
		if _, err := q.Dequeue(); err != nil {
			t.Fatalf("Dequeue() error = %v", err)
		}
	}

	stats = q.Stats()
	if stats.TotalMessages != 5 {
		t.Errorf("TotalMessages = %d, want 5", stats.TotalMessages)
	}

	if stats.PendingMessages != 3 {
		t.Errorf("PendingMessages = %d, want 3", stats.PendingMessages)
	}

	if stats.ReadMessageID != 3 {
		t.Errorf("ReadMessageID = %d, want 3", stats.ReadMessageID)
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: enqueue messages
	q1, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() first error = %v", err)
	}

	messages := []string{"msg1", "msg2", "msg3"}
	for _, msg := range messages {
		if _, err := q1.Enqueue([]byte(msg)); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	if err := q1.Close(); err != nil {
		t.Fatalf("Close() first error = %v", err)
	}

	// Second session: verify messages are persisted
	q2, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() second error = %v", err)
	}
	defer func() { _ = q2.Close() }()

	stats := q2.Stats()
	if stats.TotalMessages != 3 {
		t.Errorf("Recovered TotalMessages = %d, want 3", stats.TotalMessages)
	}

	// Dequeue and verify
	for i, expected := range messages {
		msg, err := q2.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue() %d error = %v", i, err)
		}

		if string(msg.Payload) != expected {
			t.Errorf("Dequeue() %d payload = %s, want %s", i, msg.Payload, expected)
		}
	}
}

func TestDefaultOptions(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)

	if opts.SegmentOptions == nil {
		t.Error("SegmentOptions should not be nil")
	}

	if opts.SegmentOptions.Directory != tmpDir {
		t.Errorf("SegmentOptions.Directory = %s, want %s", opts.SegmentOptions.Directory, tmpDir)
	}

	if opts.AutoSync {
		t.Error("AutoSync should be false by default")
	}

	if opts.SyncInterval != 1*time.Second {
		t.Errorf("SyncInterval = %v, want 1s", opts.SyncInterval)
	}
}

func TestQueueFiles(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Enqueue a message
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify queue files exist
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected queue files to be created")
	}

	// Should have at least segment and index files
	hasSegment := false
	hasIndex := false

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".log" {
			hasSegment = true
		}
		if filepath.Ext(file.Name()) == ".idx" {
			hasIndex = true
		}
	}

	if !hasSegment {
		t.Error("Expected segment (.log) file to exist")
	}

	if !hasIndex {
		t.Error("Expected index (.idx) file to exist")
	}
}

// TestStream_Basic tests basic streaming functionality
func TestStream_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages before streaming
	messages := []string{"msg1", "msg2", "msg3"}
	for _, msg := range messages {
		if _, err := q.Enqueue([]byte(msg)); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

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

	err = q.Stream(ctx, handler)

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
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue many messages
	for i := 0; i < 100; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

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

	err = q.Stream(ctx, handler)

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
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages
	for i := 0; i < 10; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

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

	err = q.Stream(ctx, handler)

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
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	handlerCalled := false
	handler := func(msg *Message) error {
		handlerCalled = true
		return nil
	}

	err = q.Stream(ctx, handler)

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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var receivedCount atomic.Int32
	totalToSend := 20

	handler := func(msg *Message) error {
		receivedCount.Add(1)
		// Cancel after receiving all messages
		if receivedCount.Load() >= int32(totalToSend) {
			cancel()
		}
		return nil
	}

	// Start streaming in background
	var streamErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = q.Stream(ctx, handler)
	}()

	// Give stream a moment to start
	time.Sleep(100 * time.Millisecond)

	// Produce messages while streaming
	for i := 0; i < totalToSend; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
		// Small delay between messages
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for stream to finish
	wg.Wait()

	// Should complete successfully (context canceled after all messages)
	if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
		t.Errorf("Stream() error = %v", streamErr)
	}

	// Should have received all messages
	if receivedCount.Load() != int32(totalToSend) {
		t.Errorf("Received %d messages, want %d", receivedCount.Load(), totalToSend)
	}
}

// TestStream_NilHandler tests that nil handler returns error
func TestStream_NilHandler(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	ctx := context.Background()

	err = q.Stream(ctx, nil)
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

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Enqueue a message
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// Close queue
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	ctx := context.Background()
	handler := func(msg *Message) error {
		return nil
	}

	err = q.Stream(ctx, handler)
	if err == nil {
		t.Error("Stream() after Close() should fail")
	}
}
