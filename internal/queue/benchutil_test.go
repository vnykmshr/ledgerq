package queue

import (
	"fmt"
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

// populateQueueWithPriority pre-populates a queue with n messages with cycling priorities.
func populateQueueWithPriority(b *testing.B, q *Queue, n int) {
	b.Helper()

	payload := benchPayload()
	for i := 0; i < n; i++ {
		priority := uint8(i % 3) // Cycle through priorities
		if _, err := q.EnqueueWithPriority(payload, priority); err != nil {
			b.Fatalf("failed to populate queue with priority: %v", err)
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

// benchBatchWithOptions creates a batch with all options for benchmarking.
func benchBatchWithOptions(batchSize int) []BatchEnqueueOptions {
	messages := make([]BatchEnqueueOptions, batchSize)
	for i := 0; i < batchSize; i++ {
		messages[i] = BatchEnqueueOptions{
			Payload:  benchPayload(),
			Priority: uint8(i % 3), // Mix of priorities
			TTL:      0,
			Headers:  nil,
		}
	}
	return messages
}

// benchBatchWithAllFeatures creates a batch with priority, TTL, and headers.
func benchBatchWithAllFeatures(batchSize int) []BatchEnqueueOptions {
	messages := make([]BatchEnqueueOptions, batchSize)
	for i := 0; i < batchSize; i++ {
		messages[i] = BatchEnqueueOptions{
			Payload:  benchPayload(),
			Priority: uint8(i % 3),
			TTL:      0, // Use 0 for benchmarks (no expiration overhead)
			Headers: map[string]string{
				"source": fmt.Sprintf("producer-%d", i%5),
				"type":   "benchmark",
			},
		}
	}
	return messages
}
