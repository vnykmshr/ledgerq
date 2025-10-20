package queue

import (
	"testing"
)

func TestDequeue(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue a message
	payload := []byte("test message")
	_, err := q.Enqueue(payload)
	assertNoError(t, err)

	// Dequeue it
	msg, err := q.Dequeue()
	assertNoError(t, err)

	if msg.ID != 1 {
		t.Errorf("Message ID = %d, want 1", msg.ID)
	}

	if string(msg.Payload) != string(payload) {
		t.Errorf("Message Payload = %s, want %s", msg.Payload, payload)
	}

	if msg.Timestamp == 0 {
		t.Error("Message Timestamp = 0, want > 0")
	}

	assertStatsDetailed(t, q, 1, 0, 2, 2)
}

func TestDequeue_Empty(t *testing.T) {
	q := setupQueue(t, nil)

	// Try to dequeue from empty queue
	_, err := q.Dequeue()
	if err == nil {
		t.Error("Dequeue() on empty queue should fail")
	}
}

func TestDequeue_AfterConsumeAll(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue and dequeue one message
	enqueueN(t, q, 1)
	dequeueN(t, q, 1)

	// Try to dequeue again (should fail)
	_, err := q.Dequeue()
	if err == nil {
		t.Error("Dequeue() after consuming all messages should fail")
	}
}
