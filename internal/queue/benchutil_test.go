package queue

import (
	"testing"
)

// setupBenchQueue creates a test queue for benchmarks with optional options.
// The queue is automatically closed when the benchmark completes.
func setupBenchQueue(b *testing.B, opts *Options) *Queue {
	b.Helper()

	tmpDir := b.TempDir()
	if opts == nil {
		opts = DefaultOptions(tmpDir)
		opts.AutoSync = false // Default to no sync for benchmarks
	} else if opts.SegmentOptions != nil && opts.SegmentOptions.Directory == "" {
		opts.SegmentOptions.Directory = tmpDir
	}

	q, err := Open(tmpDir, opts)
	if err != nil {
		b.Fatalf("failed to create queue: %v", err)
	}

	b.Cleanup(func() { _ = q.Close() })

	return q
}

// benchPayload returns a standard benchmark payload.
func benchPayload() []byte {
	return []byte("benchmark message payload")
}

// benchPayloads returns n benchmark payloads.
func benchPayloads(n int) [][]byte {
	payloads := make([][]byte, n)
	for i := 0; i < n; i++ {
		payloads[i] = benchPayload()
	}
	return payloads
}

// populateQueue pre-populates a queue with n messages for benchmark setup.
func populateQueue(b *testing.B, q *Queue, n int) {
	b.Helper()

	payload := benchPayload()
	for i := 0; i < n; i++ {
		if _, err := q.Enqueue(payload); err != nil {
			b.Fatalf("failed to populate queue: %v", err)
		}
	}
	_ = q.Sync()
}

// populateQueueBatch pre-populates a queue using batch operations.
func populateQueueBatch(b *testing.B, q *Queue, totalMessages int, batchSize int) {
	b.Helper()

	for i := 0; i < totalMessages; i += batchSize {
		remaining := totalMessages - i
		if remaining > batchSize {
			remaining = batchSize
		}

		batch := make([][]byte, remaining)
		for j := 0; j < remaining; j++ {
			batch[j] = benchPayload()
		}

		if _, err := q.EnqueueBatch(batch); err != nil {
			b.Fatalf("failed to batch populate queue: %v", err)
		}
	}
	_ = q.Sync()
}
