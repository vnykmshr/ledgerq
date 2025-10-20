package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Test that DLQ is initialized when DLQPath is configured
func TestQueue_DLQInitialization(t *testing.T) {
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "main")
	dlqDir := filepath.Join(baseDir, "dlq")

	opts := DefaultOptions(dir)
	opts.DLQPath = dlqDir
	opts.MaxRetries = 3

	// Open queue with DLQ configured
	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()
	
	// Verify DLQ was initialized
	if q.dlq == nil {
		t.Error("DLQ should be initialized when DLQPath is configured")
	}
	
	if q.retryTracker == nil {
		t.Error("RetryTracker should be initialized when DLQPath is configured")
	}
	
	// Verify DLQ directory was created
	if _, err := os.Stat(dlqDir); os.IsNotExist(err) {
		t.Error("DLQ directory should be created")
	}

	// Note: Retry state file is only created when there's state to save,
	// so we don't check for its existence on initialization
}

// Test that DLQ is NOT initialized when DLQPath is empty
func TestQueue_DLQNotInitializedByDefault(t *testing.T) {
	dir := t.TempDir()
	
	opts := DefaultOptions(dir)
	// DLQPath is empty by default
	
	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()
	
	// Verify DLQ was NOT initialized
	if q.dlq != nil {
		t.Error("DLQ should not be initialized when DLQPath is empty")
	}
	
	if q.retryTracker != nil {
		t.Error("RetryTracker should not be initialized when DLQPath is empty")
	}
}

// Test that DLQ queue itself doesn't have DLQ configured (prevent infinite recursion)
func TestQueue_DLQNoRecursion(t *testing.T) {
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "main")
	dlqDir := filepath.Join(baseDir, "dlq")

	opts := DefaultOptions(dir)
	opts.DLQPath = dlqDir
	opts.MaxRetries = 3

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()
	
	// Verify main queue has DLQ
	if q.dlq == nil {
		t.Fatal("Main queue should have DLQ")
	}
	
	// Verify DLQ doesn't have its own DLQ (no recursion)
	if q.dlq.dlq != nil {
		t.Error("DLQ queue should not have its own DLQ (infinite recursion prevention)")
	}
	
	if q.dlq.retryTracker != nil {
		t.Error("DLQ queue should not have retry tracker (infinite recursion prevention)")
	}
	
	// Verify DLQ options
	if q.dlq.opts.DLQPath != "" {
		t.Errorf("DLQ queue should have empty DLQPath, got: %s", q.dlq.opts.DLQPath)
	}
	
	if q.dlq.opts.MaxRetries != 0 {
		t.Errorf("DLQ queue should have MaxRetries=0, got: %d", q.dlq.opts.MaxRetries)
	}
}

// Test that Ack removes message from retry tracking
func TestQueue_Ack(t *testing.T) {
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "main")
	dlqDir := filepath.Join(baseDir, "dlq")

	opts := DefaultOptions(dir)
	opts.DLQPath = dlqDir
	opts.MaxRetries = 3

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()

	// Enqueue a message
	_, err = q.Enqueue([]byte("test message"))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue to get the message ID
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	msgID := msg.ID

	// Nack the message once to start tracking
	if err := q.Nack(msgID, "test failure"); err != nil {
		t.Fatalf("failed to nack: %v", err)
	}

	// Verify retry tracking exists
	info := q.retryTracker.GetInfo(msgID)
	if info == nil {
		t.Fatal("expected retry info to exist")
	}
	if info.RetryCount != 1 {
		t.Errorf("expected retry count 1, got %d", info.RetryCount)
	}

	// Ack the message
	if err := q.Ack(msgID); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}

	// Verify retry tracking was removed
	info = q.retryTracker.GetInfo(msgID)
	if info != nil {
		t.Error("expected retry info to be removed after Ack")
	}
}

// Test that Nack increments retry count
func TestQueue_Nack_IncrementsRetryCount(t *testing.T) {
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "main")
	dlqDir := filepath.Join(baseDir, "dlq")

	opts := DefaultOptions(dir)
	opts.DLQPath = dlqDir
	opts.MaxRetries = 3

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()

	// Enqueue a message
	_, err = q.Enqueue([]byte("test message"))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue to get the message ID
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	msgID := msg.ID

	// Nack the message multiple times
	for i := 1; i <= 2; i++ {
		if err := q.Nack(msgID, fmt.Sprintf("failure %d", i)); err != nil {
			t.Fatalf("failed to nack: %v", err)
		}

		info := q.retryTracker.GetInfo(msgID)
		if info == nil {
			t.Fatal("expected retry info to exist")
		}
		if info.RetryCount != i {
			t.Errorf("expected retry count %d, got %d", i, info.RetryCount)
		}
		if info.FailureReason != fmt.Sprintf("failure %d", i) {
			t.Errorf("expected failure reason 'failure %d', got '%s'", i, info.FailureReason)
		}
	}
}

// Test that message is moved to DLQ after exceeding max retries
func TestQueue_Nack_MovesToDLQ(t *testing.T) {
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "main")
	dlqDir := filepath.Join(baseDir, "dlq")

	opts := DefaultOptions(dir)
	opts.DLQPath = dlqDir
	opts.MaxRetries = 3

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()

	// Enqueue a message
	payload := []byte("test message for DLQ")
	_, err = q.Enqueue(payload)
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue to get the message ID
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	msgID := msg.ID

	// Nack the message MaxRetries times
	for i := 1; i <= opts.MaxRetries; i++ {
		if err := q.Nack(msgID, fmt.Sprintf("failure %d", i)); err != nil {
			t.Fatalf("failed to nack %d: %v", i, err)
		}
	}

	// Verify retry tracking was cleaned up
	info := q.retryTracker.GetInfo(msgID)
	if info != nil {
		t.Error("expected retry tracking to be cleaned up after moving to DLQ")
	}

	// Verify message is in DLQ
	dlq := q.GetDLQ()
	if dlq == nil {
		t.Fatal("DLQ should not be nil")
	}

	dlqStats := dlq.Stats()
	if dlqStats.PendingMessages != 1 {
		t.Errorf("expected 1 message in DLQ, got %d", dlqStats.PendingMessages)
	}

	// Dequeue from DLQ and verify payload and metadata
	dlqMsg, err := dlq.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue from DLQ: %v", err)
	}

	// Check payload
	if string(dlqMsg.Payload) != string(payload) {
		t.Errorf("DLQ message payload mismatch: got '%s', expected '%s'",
			string(dlqMsg.Payload), string(payload))
	}

	// Check DLQ metadata headers
	if dlqMsg.Headers == nil {
		t.Fatal("expected DLQ headers to be set")
	}

	if dlqMsg.Headers["dlq.original_msg_id"] != fmt.Sprintf("%d", msgID) {
		t.Errorf("expected dlq.original_msg_id=%d, got %s", msgID, dlqMsg.Headers["dlq.original_msg_id"])
	}

	if dlqMsg.Headers["dlq.retry_count"] != "3" {
		t.Errorf("expected dlq.retry_count=3, got %s", dlqMsg.Headers["dlq.retry_count"])
	}

	if dlqMsg.Headers["dlq.failure_reason"] != "failure 3" {
		t.Errorf("expected dlq.failure_reason='failure 3', got '%s'", dlqMsg.Headers["dlq.failure_reason"])
	}

	if dlqMsg.Headers["dlq.last_failure"] == "" {
		t.Error("expected dlq.last_failure to be set")
	}
}

// Test that Ack/Nack are no-ops when DLQ is not configured
func TestQueue_AckNack_NoDLQ(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	// DLQ not configured

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()

	// Enqueue a message
	_, err = q.Enqueue([]byte("test message"))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue to get the message ID
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	msgID := msg.ID

	// Ack should be a no-op
	if err := q.Ack(msgID); err != nil {
		t.Errorf("Ack should be no-op when DLQ not configured, got error: %v", err)
	}

	// Nack should be a no-op
	if err := q.Nack(msgID, "test failure"); err != nil {
		t.Errorf("Nack should be no-op when DLQ not configured, got error: %v", err)
	}
}

// Test GetDLQ returns nil when DLQ is not configured
func TestQueue_GetDLQ_NotConfigured(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	// DLQ not configured

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()

	dlq := q.GetDLQ()
	if dlq != nil {
		t.Error("expected GetDLQ to return nil when DLQ not configured")
	}
}

// Test GetDLQ returns valid queue when DLQ is configured
func TestQueue_GetDLQ_Configured(t *testing.T) {
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "main")
	dlqDir := filepath.Join(baseDir, "dlq")

	opts := DefaultOptions(dir)
	opts.DLQPath = dlqDir
	opts.MaxRetries = 3

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()

	dlq := q.GetDLQ()
	if dlq == nil {
		t.Fatal("expected GetDLQ to return valid queue")
	}

	// Verify DLQ can be used
	stats := dlq.Stats()
	if stats == nil {
		t.Error("expected valid DLQ stats")
	}
}

// Test RequeueFromDLQ moves message back to main queue
func TestQueue_RequeueFromDLQ(t *testing.T) {
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "main")
	dlqDir := filepath.Join(baseDir, "dlq")

	opts := DefaultOptions(dir)
	opts.DLQPath = dlqDir
	opts.MaxRetries = 3

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()

	// Enqueue a message with headers
	payload := []byte("test message for requeue")
	headers := map[string]string{
		"trace-id": "12345",
		"app":      "test",
	}
	_, err = q.EnqueueWithHeaders(payload, headers)
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue to get the message ID
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	msgID := msg.ID

	// Move message to DLQ by exceeding retries
	for i := 1; i <= opts.MaxRetries; i++ {
		if err := q.Nack(msgID, "test failure"); err != nil {
			t.Fatalf("failed to nack: %v", err)
		}
	}

	// Get DLQ message ID
	dlq := q.GetDLQ()
	dlqMsg, err := dlq.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue from DLQ: %v", err)
	}
	dlqMsgID := dlqMsg.ID

	// Requeue from DLQ
	if err := q.RequeueFromDLQ(dlqMsgID); err != nil {
		t.Fatalf("failed to requeue from DLQ: %v", err)
	}

	// Verify message is back in main queue
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue from main queue: %v", err)
	}

	// Check payload
	if string(msg.Payload) != string(payload) {
		t.Errorf("requeued message payload mismatch: got '%s', expected '%s'",
			string(msg.Payload), string(payload))
	}

	// Check original headers are preserved
	if msg.Headers["trace-id"] != "12345" {
		t.Errorf("expected trace-id header preserved, got %v", msg.Headers["trace-id"])
	}
	if msg.Headers["app"] != "test" {
		t.Errorf("expected app header preserved, got %v", msg.Headers["app"])
	}

	// Check DLQ headers are NOT in requeued message
	if _, exists := msg.Headers["dlq.original_msg_id"]; exists {
		t.Error("expected DLQ headers to be removed from requeued message")
	}
	if _, exists := msg.Headers["dlq.retry_count"]; exists {
		t.Error("expected DLQ headers to be removed from requeued message")
	}
}

// Test RequeueFromDLQ fails when DLQ not configured
func TestQueue_RequeueFromDLQ_NoDLQ(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	// DLQ not configured

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	defer q.Close()

	// Try to requeue
	err = q.RequeueFromDLQ(1)
	if err == nil {
		t.Error("expected error when calling RequeueFromDLQ without DLQ configured")
	}
	if err.Error() != "DLQ not configured" {
		t.Errorf("expected 'DLQ not configured' error, got: %v", err)
	}
}

// Test message preservation across queue restart
func TestQueue_DLQ_PersistenceAcrossRestart(t *testing.T) {
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "main")
	dlqDir := filepath.Join(baseDir, "dlq")

	opts := DefaultOptions(dir)
	opts.DLQPath = dlqDir
	opts.MaxRetries = 3

	payload := []byte("persistent message")
	var dlqMsgID uint64

	// First session: move message to DLQ
	{
		q, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("failed to open queue: %v", err)
		}

		_, err = q.Enqueue(payload)
		if err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Dequeue to get the message ID
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue: %v", err)
		}
		msgID := msg.ID

		// Move to DLQ
		for i := 1; i <= opts.MaxRetries; i++ {
			if err := q.Nack(msgID, "test failure"); err != nil {
				t.Fatalf("failed to nack: %v", err)
			}
		}

		// Get DLQ message ID
		dlq := q.GetDLQ()
		dlqMsg, err := dlq.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue from DLQ: %v", err)
		}
		dlqMsgID = dlqMsg.ID

		// Note: The message was dequeued, so readMsgID is already advanced

		q.Close()
	}

	// Second session: verify DLQ message persisted
	{
		q, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("failed to open queue: %v", err)
		}
		defer q.Close()

		// Reset DLQ read position to read the message again
		dlq := q.GetDLQ()
		if err := dlq.SeekToMessageID(dlqMsgID); err != nil {
			t.Fatalf("failed to seek in DLQ: %v", err)
		}

		// Verify message is still in DLQ
		dlqMsg, err := dlq.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue from DLQ after restart: %v", err)
		}

		if string(dlqMsg.Payload) != string(payload) {
			t.Errorf("DLQ message payload mismatch after restart: got '%s', expected '%s'",
				string(dlqMsg.Payload), string(payload))
		}
	}
}
