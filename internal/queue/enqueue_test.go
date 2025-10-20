package queue

import (
	"testing"
)

func TestEnqueue(t *testing.T) {
	q := setupQueue(t, nil)

	payload := []byte("test message")
	offset, err := q.Enqueue(payload)
	assertNoError(t, err)

	if offset == 0 {
		t.Error("Enqueue() offset = 0, want > 0")
	}

	assertStatsDetailed(t, q, 1, 1, 2, 1)
}

func TestEnqueue_Multiple(t *testing.T) {
	q := setupQueue(t, nil)
	enqueueN(t, q, 10)
	assertStats(t, q, 10, 10)
}
