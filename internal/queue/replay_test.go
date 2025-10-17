package queue

import (
	"fmt"
	"testing"
	"time"
)

func TestSeekToMessageID(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue 10 messages
	for i := 0; i < 10; i++ {
		payload := []byte(fmt.Sprintf("message %d", i+1))
		if _, err := q.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Seek to message ID 5
	if err := q.SeekToMessageID(5); err != nil {
		t.Fatalf("SeekToMessageID(5) error = %v", err)
	}

	// Dequeue should return message 5
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if msg.ID != 5 {
		t.Errorf("Dequeue() after SeekToMessageID(5) returned ID %d, want 5", msg.ID)
	}

	if string(msg.Payload) != "message 5" {
		t.Errorf("Dequeue() after SeekToMessageID(5) returned payload %s, want 'message 5'", msg.Payload)
	}

	// Next dequeue should return message 6
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() second error = %v", err)
	}

	if msg.ID != 6 {
		t.Errorf("Second Dequeue() returned ID %d, want 6", msg.ID)
	}
}

func TestSeekToMessageID_Beginning(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Dequeue a few messages
	for i := 0; i < 3; i++ {
		if _, err := q.Dequeue(); err != nil {
			t.Fatalf("Dequeue() error = %v", err)
		}
	}

	// Seek back to the beginning
	if err := q.SeekToMessageID(1); err != nil {
		t.Fatalf("SeekToMessageID(1) error = %v", err)
	}

	// Should be able to re-read from the start
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() after seek error = %v", err)
	}

	if msg.ID != 1 {
		t.Errorf("Dequeue() after SeekToMessageID(1) returned ID %d, want 1", msg.ID)
	}
}

func TestSeekToMessageID_InvalidID(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue 5 messages
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Try to seek to ID 0 (invalid)
	err = q.SeekToMessageID(0)
	if err == nil {
		t.Error("SeekToMessageID(0) should fail")
	}

	// Try to seek to ID that hasn't been written yet
	err = q.SeekToMessageID(100)
	if err == nil {
		t.Error("SeekToMessageID(100) should fail when only 5 messages exist")
	}
}

func TestSeekToMessageID_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Try to seek after close
	err = q.SeekToMessageID(1)
	if err == nil {
		t.Error("SeekToMessageID() after Close() should fail")
	}
}

func TestSeekToMessageID_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: enqueue messages
	q1, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() first error = %v", err)
	}

	for i := 0; i < 10; i++ {
		if _, err := q1.Enqueue([]byte(fmt.Sprintf("msg%d", i+1))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	if err := q1.Close(); err != nil {
		t.Fatalf("Close() first error = %v", err)
	}

	// Second session: seek and read
	q2, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() second error = %v", err)
	}
	defer func() { _ = q2.Close() }()

	// Seek to message 7
	if err := q2.SeekToMessageID(7); err != nil {
		t.Fatalf("SeekToMessageID(7) error = %v", err)
	}

	// Should read message 7
	msg, err := q2.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if msg.ID != 7 {
		t.Errorf("Dequeue() after seek returned ID %d, want 7", msg.ID)
	}

	if string(msg.Payload) != "msg7" {
		t.Errorf("Dequeue() payload = %s, want msg7", msg.Payload)
	}
}

func TestSeekToMessageID_WithBatch(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue 20 messages
	for i := 0; i < 20; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg%d", i+1))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Seek to message 10
	if err := q.SeekToMessageID(10); err != nil {
		t.Fatalf("SeekToMessageID(10) error = %v", err)
	}

	// Dequeue batch should start from message 10
	messages, err := q.DequeueBatch(5)
	if err != nil {
		t.Fatalf("DequeueBatch() error = %v", err)
	}

	if len(messages) != 5 {
		t.Errorf("DequeueBatch() returned %d messages, want 5", len(messages))
	}

	// Verify IDs are 10-14
	for i, msg := range messages {
		expectedID := uint64(10 + i)
		if msg.ID != expectedID {
			t.Errorf("Message[%d] ID = %d, want %d", i, msg.ID, expectedID)
		}
	}
}

func TestSeekToMessageID_MultipleSeeks(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue 10 messages
	for i := 0; i < 10; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg%d", i+1))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Seek to 5, read one
	if err := q.SeekToMessageID(5); err != nil {
		t.Fatalf("SeekToMessageID(5) error = %v", err)
	}

	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}
	if msg.ID != 5 {
		t.Errorf("First dequeue ID = %d, want 5", msg.ID)
	}

	// Seek to 2 (backward)
	if err := q.SeekToMessageID(2); err != nil {
		t.Fatalf("SeekToMessageID(2) error = %v", err)
	}

	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() second error = %v", err)
	}
	if msg.ID != 2 {
		t.Errorf("Second dequeue ID = %d, want 2", msg.ID)
	}

	// Seek to 8 (forward)
	if err := q.SeekToMessageID(8); err != nil {
		t.Fatalf("SeekToMessageID(8) error = %v", err)
	}

	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() third error = %v", err)
	}
	if msg.ID != 8 {
		t.Errorf("Third dequeue ID = %d, want 8", msg.ID)
	}
}

func TestSeekToTimestamp(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages with different timestamps
	timestamps := make([]int64, 5)
	for i := 0; i < 5; i++ {
		timestamps[i] = time.Now().UnixNano()
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg%d", i+1))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// Seek to timestamp of message 3 (index 2)
	if err := q.SeekToTimestamp(timestamps[2]); err != nil {
		t.Fatalf("SeekToTimestamp() error = %v", err)
	}

	// Should read message 3
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if msg.ID != 3 {
		t.Errorf("Dequeue() after SeekToTimestamp returned ID %d, want 3", msg.ID)
	}
}

func TestSeekToTimestamp_BeforeAll(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Record time before enqueuing
	beforeTime := time.Now().UnixNano()
	time.Sleep(10 * time.Millisecond)

	// Enqueue messages
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg%d", i+1))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Seek to before all messages
	if err := q.SeekToTimestamp(beforeTime); err != nil {
		t.Fatalf("SeekToTimestamp(beforeTime) error = %v", err)
	}

	// Should read first message
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if msg.ID != 1 {
		t.Errorf("Dequeue() after SeekToTimestamp(beforeTime) returned ID %d, want 1", msg.ID)
	}
}

func TestSeekToTimestamp_AfterAll(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg%d", i+1))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Wait and get time after all messages
	time.Sleep(10 * time.Millisecond)
	afterTime := time.Now().UnixNano()

	// Seek to after all messages
	err = q.SeekToTimestamp(afterTime)
	if err == nil {
		t.Error("SeekToTimestamp(afterTime) should fail when no messages after timestamp")
	}
}

func TestSeekToTimestamp_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Try to seek after close
	err = q.SeekToTimestamp(time.Now().UnixNano())
	if err == nil {
		t.Error("SeekToTimestamp() after Close() should fail")
	}
}

func TestSeekToTimestamp_WithBatch(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages with timestamps
	timestamps := make([]int64, 10)
	for i := 0; i < 10; i++ {
		timestamps[i] = time.Now().UnixNano()
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg%d", i+1))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Seek to timestamp around message 5
	if err := q.SeekToTimestamp(timestamps[4]); err != nil {
		t.Fatalf("SeekToTimestamp() error = %v", err)
	}

	// Dequeue batch should start from around message 5
	messages, err := q.DequeueBatch(3)
	if err != nil {
		t.Fatalf("DequeueBatch() error = %v", err)
	}

	if len(messages) != 3 {
		t.Errorf("DequeueBatch() returned %d messages, want 3", len(messages))
	}

	// First message should be ID 5 or close to it
	if messages[0].ID < 5 || messages[0].ID > 6 {
		t.Errorf("First message ID = %d, want 5 or 6", messages[0].ID)
	}
}
