package queue

import (
	"testing"
)

func TestSync(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions(tmpDir)
	opts.AutoSync = false // Disable auto-sync

	q := setupQueue(t, opts)
	enqueueN(t, q, 1)

	// Manual sync
	assertNoError(t, q.Sync())
}

func TestStats(t *testing.T) {
	q := setupQueue(t, nil)

	// Initial stats
	assertStats(t, q, 0, 0)

	// Enqueue some messages
	enqueueN(t, q, 5)
	assertStats(t, q, 5, 5)

	// Dequeue some messages
	dequeueN(t, q, 2)
	assertStatsDetailed(t, q, 5, 3, 6, 3)
}
