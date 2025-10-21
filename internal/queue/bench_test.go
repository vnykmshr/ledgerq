package queue

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkEnqueue(b *testing.B) {
	q := setupBenchQueue(b, nil)
	payload := benchPayload()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(payload); err != nil {
			b.Fatalf("Enqueue() error = %v", err)
		}
	}
}

func BenchmarkEnqueueBatch_10(b *testing.B) {
	benchmarkEnqueueBatch(b, 10)
}

func BenchmarkEnqueueBatch_100(b *testing.B) {
	benchmarkEnqueueBatch(b, 100)
}

func BenchmarkEnqueueBatch_1000(b *testing.B) {
	benchmarkEnqueueBatch(b, 1000)
}

func benchmarkEnqueueBatch(b *testing.B, batchSize int) {
	q := setupBenchQueue(b, nil)
	payloads := benchPayloads(batchSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatch(payloads); err != nil {
			b.Fatalf("EnqueueBatch() error = %v", err)
		}
	}
}

func BenchmarkEnqueueWithSync(b *testing.B) {
	opts := &Options{AutoSync: true}
	q := setupBenchQueue(b, opts)
	payload := benchPayload()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(payload); err != nil {
			b.Fatalf("Enqueue() error = %v", err)
		}
	}
}

func BenchmarkEnqueueBatchWithSync_10(b *testing.B) {
	benchmarkEnqueueBatchWithSync(b, 10)
}

func BenchmarkEnqueueBatchWithSync_100(b *testing.B) {
	benchmarkEnqueueBatchWithSync(b, 100)
}

func benchmarkEnqueueBatchWithSync(b *testing.B, batchSize int) {
	opts := &Options{AutoSync: true}
	q := setupBenchQueue(b, opts)
	payloads := benchPayloads(batchSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatch(payloads); err != nil {
			b.Fatalf("EnqueueBatch() error = %v", err)
		}
	}
}

func BenchmarkDequeue(b *testing.B) {
	q := setupBenchQueue(b, nil)
	populateQueue(b, q, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Dequeue(); err != nil {
			b.Fatalf("Dequeue() error = %v", err)
		}
	}
}

func BenchmarkDequeueBatch_10(b *testing.B) {
	benchmarkDequeueBatch(b, 10)
}

func BenchmarkDequeueBatch_100(b *testing.B) {
	benchmarkDequeueBatch(b, 100)
}

func BenchmarkDequeueBatch_1000(b *testing.B) {
	benchmarkDequeueBatch(b, 1000)
}

func benchmarkDequeueBatch(b *testing.B, batchSize int) {
	q := setupBenchQueue(b, nil)
	totalMessages := b.N * batchSize
	populateQueueBatch(b, q, totalMessages, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.DequeueBatch(batchSize); err != nil {
			b.Fatalf("DequeueBatch() error = %v", err)
		}
	}
}

func BenchmarkRoundtrip(b *testing.B) {
	q := setupBenchQueue(b, nil)
	payload := benchPayload()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(payload); err != nil {
			b.Fatalf("Enqueue() error = %v", err)
		}
		if _, err := q.Dequeue(); err != nil {
			b.Fatalf("Dequeue() error = %v", err)
		}
	}
}

func BenchmarkRoundtripBatch_10(b *testing.B) {
	benchmarkRoundtripBatch(b, 10)
}

func BenchmarkRoundtripBatch_100(b *testing.B) {
	benchmarkRoundtripBatch(b, 100)
}

func benchmarkRoundtripBatch(b *testing.B, batchSize int) {
	q := setupBenchQueue(b, nil)
	payloads := benchPayloads(batchSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatch(payloads); err != nil {
			b.Fatalf("EnqueueBatch() error = %v", err)
		}
		if _, err := q.DequeueBatch(batchSize); err != nil {
			b.Fatalf("DequeueBatch() error = %v", err)
		}
	}
}

// Benchmark Stats operation
func BenchmarkStats(b *testing.B) {
	q := setupBenchQueue(b, nil)
	populateQueue(b, q, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Stats()
	}
}

// Benchmark replay operations
func BenchmarkSeekToMessageID(b *testing.B) {
	q := setupBenchQueue(b, nil)
	populateQueue(b, q, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Seek to middle
		if err := q.SeekToMessageID(500); err != nil {
			b.Fatalf("SeekToMessageID() error = %v", err)
		}
	}
}

func BenchmarkSeekToTimestamp(b *testing.B) {
	q := setupBenchQueue(b, nil)

	// Enqueue 1000 messages, capturing a timestamp in the middle
	var midTimestamp int64
	payload := benchPayload()
	for i := 0; i < 1000; i++ {
		if i == 500 {
			midTimestamp = time.Now().UnixNano()
		}
		if _, err := q.Enqueue(payload); err != nil {
			b.Fatalf("Enqueue() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.SeekToTimestamp(midTimestamp)
	}
}

func BenchmarkReplayAndRead(b *testing.B) {
	q := setupBenchQueue(b, nil)
	populateQueue(b, q, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.SeekToMessageID(1); err != nil {
			b.Fatalf("SeekToMessageID() error = %v", err)
		}
		for j := 0; j < 10; j++ {
			if _, err := q.Dequeue(); err != nil {
				b.Fatalf("Dequeue() error = %v", err)
			}
		}
	}
}

// Concurrent benchmarks

func BenchmarkConcurrentEnqueue_2Writers(b *testing.B) {
	benchmarkConcurrentEnqueue(b, 2)
}

func BenchmarkConcurrentEnqueue_4Writers(b *testing.B) {
	benchmarkConcurrentEnqueue(b, 4)
}

func BenchmarkConcurrentEnqueue_8Writers(b *testing.B) {
	benchmarkConcurrentEnqueue(b, 8)
}

func benchmarkConcurrentEnqueue(b *testing.B, numWriters int) {
	q := setupBenchQueue(b, nil)
	payload := benchPayload()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.Enqueue(payload); err != nil {
				b.Fatalf("Enqueue() error = %v", err)
			}
		}
	})
}

func BenchmarkConcurrentDequeue_2Readers(b *testing.B) {
	benchmarkConcurrentDequeue(b, 2)
}

func BenchmarkConcurrentDequeue_4Readers(b *testing.B) {
	benchmarkConcurrentDequeue(b, 4)
}

func BenchmarkConcurrentDequeue_8Readers(b *testing.B) {
	benchmarkConcurrentDequeue(b, 8)
}

func benchmarkConcurrentDequeue(b *testing.B, numReaders int) {
	q := setupBenchQueue(b, nil)
	totalMessages := b.N * numReaders
	populateQueue(b, q, totalMessages)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.Dequeue(); err != nil {
				// Expected: some readers will hit end of queue
				return
			}
		}
	})
}

func BenchmarkConcurrentMixed_2P2C(b *testing.B) {
	benchmarkConcurrentMixed(b, 2, 2)
}

func BenchmarkConcurrentMixed_4P4C(b *testing.B) {
	benchmarkConcurrentMixed(b, 4, 4)
}

func BenchmarkConcurrentMixed_8P8C(b *testing.B) {
	benchmarkConcurrentMixed(b, 8, 8)
}

func benchmarkConcurrentMixed(b *testing.B, numProducers, numConsumers int) {
	q := setupBenchQueue(b, nil)
	payload := benchPayload()

	b.ResetTimer()

	// Start producers
	producerDone := make(chan struct{})
	go func() {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = q.Enqueue(payload)
			}
		})
		close(producerDone)
	}()

	// Start consumers
	consumerDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-producerDone:
				// Drain remaining messages
				for {
					if _, err := q.Dequeue(); err != nil {
						close(consumerDone)
						return
					}
				}
			default:
				_, _ = q.Dequeue()
			}
		}
	}()

	<-producerDone
	<-consumerDone
}

func BenchmarkConcurrentBatchEnqueue_2Writers(b *testing.B) {
	benchmarkConcurrentBatchEnqueue(b, 2, 10)
}

func BenchmarkConcurrentBatchEnqueue_4Writers(b *testing.B) {
	benchmarkConcurrentBatchEnqueue(b, 4, 10)
}

func BenchmarkConcurrentBatchEnqueue_8Writers(b *testing.B) {
	benchmarkConcurrentBatchEnqueue(b, 8, 10)
}

func benchmarkConcurrentBatchEnqueue(b *testing.B, numWriters, batchSize int) {
	q := setupBenchQueue(b, nil)
	payloads := benchPayloads(batchSize)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.EnqueueBatch(payloads); err != nil {
				b.Fatalf("EnqueueBatch() error = %v", err)
			}
		}
	})
}

// Priority Queue Benchmarks (v1.1.0+)

// BenchmarkEnqueueWithPriority benchmarks single-message enqueue with priority
func BenchmarkEnqueueWithPriority(b *testing.B) {
	opts := &Options{EnablePriorities: true}
	q := setupBenchQueue(b, opts)
	payload := benchPayload()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		priority := uint8(i % 3) // Cycle through priorities
		if _, err := q.EnqueueWithPriority(payload, priority); err != nil {
			b.Fatalf("EnqueueWithPriority() error = %v", err)
		}
	}
}

// BenchmarkDequeuePriority benchmarks priority-aware dequeue
func BenchmarkDequeuePriority(b *testing.B) {
	opts := &Options{EnablePriorities: true}
	q := setupBenchQueue(b, opts)

	// Pre-populate with mixed priorities
	payload := benchPayload()
	for i := 0; i < b.N; i++ {
		priority := uint8(i % 3)
		if _, err := q.EnqueueWithPriority(payload, priority); err != nil {
			b.Fatalf("EnqueueWithPriority() error = %v", err)
		}
	}
	_ = q.Sync()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Dequeue(); err != nil {
			b.Fatalf("Dequeue() error = %v", err)
		}
	}
}

// BenchmarkPriorityVsFIFO compares priority queue vs FIFO performance
func BenchmarkPriorityVsFIFO_FIFO(b *testing.B) {
	q := setupBenchQueue(b, nil) // FIFO mode
	payload := benchPayload()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(payload); err != nil {
			b.Fatalf("Enqueue() error = %v", err)
		}
		if _, err := q.Dequeue(); err != nil {
			b.Fatalf("Dequeue() error = %v", err)
		}
	}
}

func BenchmarkPriorityVsFIFO_Priority(b *testing.B) {
	opts := &Options{EnablePriorities: true}
	q := setupBenchQueue(b, opts)
	payload := benchPayload()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		priority := uint8(i % 3)
		if _, err := q.EnqueueWithPriority(payload, priority); err != nil {
			b.Fatalf("EnqueueWithPriority() error = %v", err)
		}
		if _, err := q.Dequeue(); err != nil {
			b.Fatalf("Dequeue() error = %v", err)
		}
	}
}

// BenchmarkPriorityIndexRebuild benchmarks priority index rebuild on startup
func BenchmarkPriorityIndexRebuild_1K(b *testing.B) {
	benchmarkPriorityIndexRebuild(b, 1000)
}

func BenchmarkPriorityIndexRebuild_10K(b *testing.B) {
	benchmarkPriorityIndexRebuild(b, 10000)
}

func BenchmarkPriorityIndexRebuild_100K(b *testing.B) {
	benchmarkPriorityIndexRebuild(b, 100000)
}

func benchmarkPriorityIndexRebuild(b *testing.B, messageCount int) {
	tmpDir := b.TempDir()

	// Create queue with priority mode and populate it
	{
		opts := DefaultOptions(tmpDir)
		opts.EnablePriorities = true
		opts.AutoSync = false

		q, err := Open(tmpDir, opts)
		if err != nil {
			b.Fatalf("Open() error = %v", err)
		}

		payload := benchPayload()
		for i := 0; i < messageCount; i++ {
			priority := uint8(i % 3)
			if _, err := q.EnqueueWithPriority(payload, priority); err != nil {
				b.Fatalf("EnqueueWithPriority() error = %v", err)
			}
		}
		_ = q.Sync()
		_ = q.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reopen queue (triggers priority index rebuild)
		opts := DefaultOptions(tmpDir)
		opts.EnablePriorities = true

		q, err := Open(tmpDir, opts)
		if err != nil {
			b.Fatalf("Open() error = %v", err)
		}
		_ = q.Close()
	}
}

// BenchmarkMixedPriorities benchmarks with realistic mixed priority workload
func BenchmarkMixedPriorities(b *testing.B) {
	opts := &Options{EnablePriorities: true}
	q := setupBenchQueue(b, opts)
	payload := benchPayload()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Realistic workload: 10% high, 30% medium, 60% low
		var priority uint8
		r := i % 10
		if r == 0 {
			priority = 2 // High
		} else if r <= 3 {
			priority = 1 // Medium
		} else {
			priority = 0 // Low
		}

		if _, err := q.EnqueueWithPriority(payload, priority); err != nil {
			b.Fatalf("EnqueueWithPriority() error = %v", err)
		}

		if _, err := q.Dequeue(); err != nil {
			b.Fatalf("Dequeue() error = %v", err)
		}
	}
}

// BenchmarkBatchWithOptions benchmarks EnqueueBatchWithOptions with different batch sizes
func BenchmarkBatchWithOptions_10(b *testing.B) {
	benchmarkBatchWithOptions(b, 10)
}

func BenchmarkBatchWithOptions_100(b *testing.B) {
	benchmarkBatchWithOptions(b, 100)
}

func BenchmarkBatchWithOptions_1000(b *testing.B) {
	benchmarkBatchWithOptions(b, 1000)
}

func benchmarkBatchWithOptions(b *testing.B, batchSize int) {
	opts := &Options{EnablePriorities: true}
	q := setupBenchQueue(b, opts)

	// Prepare batch with priorities
	messages := make([]BatchEnqueueOptions, batchSize)
	for i := 0; i < batchSize; i++ {
		messages[i] = BatchEnqueueOptions{
			Payload:  benchPayload(),
			Priority: uint8(i % 3), // Mix of priorities
			TTL:      0,
			Headers:  nil,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatchWithOptions(messages); err != nil {
			b.Fatalf("EnqueueBatchWithOptions() error = %v", err)
		}
	}
}

// BenchmarkBatchWithOptions_AllFeatures benchmarks with priority, TTL, and headers
func BenchmarkBatchWithOptions_AllFeatures(b *testing.B) {
	opts := &Options{EnablePriorities: true}
	q := setupBenchQueue(b, opts)

	// Prepare batch with all features
	batchSize := 100
	messages := make([]BatchEnqueueOptions, batchSize)
	for i := 0; i < batchSize; i++ {
		messages[i] = BatchEnqueueOptions{
			Payload:  benchPayload(),
			Priority: uint8(i % 3),
			TTL:      time.Hour, // 1 hour TTL
			Headers: map[string]string{
				"source": fmt.Sprintf("producer-%d", i%5),
				"type":   "benchmark",
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatchWithOptions(messages); err != nil {
			b.Fatalf("EnqueueBatchWithOptions() error = %v", err)
		}
	}
}

// BenchmarkBatchVsBatchWithOptions compares plain batch vs batch with options
func BenchmarkBatchVsBatchWithOptions_Plain(b *testing.B) {
	q := setupBenchQueue(b, nil)
	payloads := benchPayloads(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatch(payloads); err != nil {
			b.Fatalf("EnqueueBatch() error = %v", err)
		}
	}
}

func BenchmarkBatchVsBatchWithOptions_WithOptions(b *testing.B) {
	opts := &Options{EnablePriorities: true}
	q := setupBenchQueue(b, opts)

	batchSize := 100
	messages := make([]BatchEnqueueOptions, batchSize)
	for i := 0; i < batchSize; i++ {
		messages[i] = BatchEnqueueOptions{
			Payload:  benchPayload(),
			Priority: uint8(i % 3),
			TTL:      0,
			Headers:  nil,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatchWithOptions(messages); err != nil {
			b.Fatalf("EnqueueBatchWithOptions() error = %v", err)
		}
	}
}
