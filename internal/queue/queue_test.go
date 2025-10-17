package queue

import (
	"os"
	"path/filepath"
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
