package queue

import (
	"fmt"
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	tmpDir := b.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = false // Disable auto-sync for fair comparison

	q, err := Open(tmpDir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	payload := []byte("benchmark message payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(payload); err != nil {
			b.Fatalf("Enqueue() error = %v", err)
		}
	}
	b.StopTimer()

	// Sync at the end
	_ = q.Sync()
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
	tmpDir := b.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = false

	q, err := Open(tmpDir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Prepare batch
	payloads := make([][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		payloads[i] = []byte("benchmark message payload")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatch(payloads); err != nil {
			b.Fatalf("EnqueueBatch() error = %v", err)
		}
	}
	b.StopTimer()

	// Sync at the end
	_ = q.Sync()
}

func BenchmarkEnqueueWithSync(b *testing.B) {
	tmpDir := b.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = true // Enable auto-sync

	q, err := Open(tmpDir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	payload := []byte("benchmark message payload")

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
	tmpDir := b.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = true

	q, err := Open(tmpDir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Prepare batch
	payloads := make([][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		payloads[i] = []byte("benchmark message payload")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueBatch(payloads); err != nil {
			b.Fatalf("EnqueueBatch() error = %v", err)
		}
	}
}

func BenchmarkDequeue(b *testing.B) {
	tmpDir := b.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Pre-populate queue
	payload := []byte("benchmark message payload")
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(payload); err != nil {
			b.Fatalf("Enqueue() error = %v", err)
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
	tmpDir := b.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Pre-populate queue with N batches
	payload := []byte("benchmark message payload")
	totalMessages := b.N * batchSize

	// Enqueue in batches for faster setup
	setupBatchSize := 1000
	for i := 0; i < totalMessages; i += setupBatchSize {
		remaining := totalMessages - i
		if remaining > setupBatchSize {
			remaining = setupBatchSize
		}

		batch := make([][]byte, remaining)
		for j := 0; j < remaining; j++ {
			batch[j] = payload
		}

		if _, err := q.EnqueueBatch(batch); err != nil {
			b.Fatalf("EnqueueBatch() error = %v", err)
		}
	}
	_ = q.Sync()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.DequeueBatch(batchSize); err != nil {
			b.Fatalf("DequeueBatch() error = %v", err)
		}
	}
}

func BenchmarkRoundtrip(b *testing.B) {
	tmpDir := b.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = false

	q, err := Open(tmpDir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	payload := []byte("benchmark message payload")

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
	tmpDir := b.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = false

	q, err := Open(tmpDir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Prepare batch
	payloads := make([][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		payloads[i] = []byte("benchmark message payload")
	}

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
	tmpDir := b.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Add some messages
	for i := 0; i < 100; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg%d", i))); err != nil {
			b.Fatalf("Enqueue() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Stats()
	}
}
