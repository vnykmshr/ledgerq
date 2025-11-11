package queue

import (
	"testing"
)

func TestOpen(t *testing.T) {
	q := setupQueue(t, nil)

	if q.IsClosed() {
		t.Error("IsClosed() = true, want false for new queue")
	}

	assertStatsDetailed(t, q, 0, 0, 1, 1)
}

func TestOpen_WithExistingData(t *testing.T) {
	tmpDir := t.TempDir()

	// Create queue and add some messages
	q1 := setupQueue(t, DefaultOptions(tmpDir))
	enqueueN(t, q1, 5)
	assertNoError(t, q1.Close())

	// Reopen and verify state is recovered
	q2 := setupQueue(t, DefaultOptions(tmpDir))
	assertStatsDetailed(t, q2, 5, 5, 6, 1)
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()

	q := setupQueue(t, DefaultOptions(tmpDir))
	enqueueN(t, q, 1)

	assertNoError(t, q.Close())

	if !q.IsClosed() {
		t.Error("IsClosed() = false, want true after Close()")
	}

	// Double close should be safe
	assertNoError(t, q.Close())
}

func TestOperationsAfterClose(t *testing.T) {
	tests := []struct {
		name string
		op   func(*Queue) error
	}{
		{
			name: "Enqueue",
			op: func(q *Queue) error {
				_, err := q.Enqueue([]byte("test"))
				return err
			},
		},
		{
			name: "Dequeue",
			op: func(q *Queue) error {
				_, err := q.Dequeue()
				return err
			},
		},
		{
			name: "Sync",
			op: func(q *Queue) error {
				return q.Sync()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			q := setupQueue(t, DefaultOptions(tmpDir))

			// Enqueue a message for dequeue test
			if tt.name == "Dequeue" {
				enqueueN(t, q, 1)
			}

			assertNoError(t, q.Close())

			// Try operation after close
			err := tt.op(q)
			if err == nil {
				t.Errorf("%s() after Close() should fail", tt.name)
			}
		})
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: enqueue messages
	q1 := setupQueue(t, DefaultOptions(tmpDir))
	messages := []string{"msg1", "msg2", "msg3"}
	enqueueMessages(t, q1, messages)
	assertNoError(t, q1.Close())

	// Second session: verify messages are persisted
	q2 := setupQueue(t, DefaultOptions(tmpDir))
	assertStats(t, q2, 3, 3)

	// Dequeue and verify
	dequeued := dequeueN(t, q2, 3)
	assertPayloads(t, dequeued, messages)
}
