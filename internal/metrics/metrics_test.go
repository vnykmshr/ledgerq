package metrics

import (
	"testing"
	"time"
)

func TestCollector_BasicOperations(t *testing.T) {
	c := NewCollector("test_queue")

	// Record some operations
	c.RecordEnqueue(100, 500*time.Microsecond)
	c.RecordEnqueue(200, 1*time.Millisecond)
	c.RecordDequeue(100, 2*time.Millisecond)

	snapshot := c.GetSnapshot()

	if snapshot.EnqueueTotal != 2 {
		t.Errorf("EnqueueTotal = %d, want 2", snapshot.EnqueueTotal)
	}

	if snapshot.DequeueTotal != 1 {
		t.Errorf("DequeueTotal = %d, want 1", snapshot.DequeueTotal)
	}

	if snapshot.EnqueueBytes != 300 {
		t.Errorf("EnqueueBytes = %d, want 300", snapshot.EnqueueBytes)
	}

	if snapshot.DequeueBytes != 100 {
		t.Errorf("DequeueBytes = %d, want 100", snapshot.DequeueBytes)
	}

	if snapshot.QueueName != "test_queue" {
		t.Errorf("QueueName = %s, want test_queue", snapshot.QueueName)
	}
}

func TestCollector_BatchOperations(t *testing.T) {
	c := NewCollector("test_queue")

	// Record batch operations
	c.RecordEnqueueBatch(10, 1000, 5*time.Millisecond)
	c.RecordDequeueBatch(5, 500, 3*time.Millisecond)

	snapshot := c.GetSnapshot()

	// Batch operation counts
	if snapshot.EnqueueBatch != 1 {
		t.Errorf("EnqueueBatch = %d, want 1", snapshot.EnqueueBatch)
	}

	if snapshot.DequeueBatch != 1 {
		t.Errorf("DequeueBatch = %d, want 1", snapshot.DequeueBatch)
	}

	// Total messages (batch + individual)
	if snapshot.EnqueueTotal != 10 {
		t.Errorf("EnqueueTotal = %d, want 10", snapshot.EnqueueTotal)
	}

	if snapshot.DequeueTotal != 5 {
		t.Errorf("DequeueTotal = %d, want 5", snapshot.DequeueTotal)
	}

	// Total bytes
	if snapshot.EnqueueBytes != 1000 {
		t.Errorf("EnqueueBytes = %d, want 1000", snapshot.EnqueueBytes)
	}

	if snapshot.DequeueBytes != 500 {
		t.Errorf("DequeueBytes = %d, want 500", snapshot.DequeueBytes)
	}
}

func TestCollector_Errors(t *testing.T) {
	c := NewCollector("test_queue")

	c.RecordEnqueueError()
	c.RecordEnqueueError()
	c.RecordDequeueError()

	snapshot := c.GetSnapshot()

	if snapshot.EnqueueErrors != 2 {
		t.Errorf("EnqueueErrors = %d, want 2", snapshot.EnqueueErrors)
	}

	if snapshot.DequeueErrors != 1 {
		t.Errorf("DequeueErrors = %d, want 1", snapshot.DequeueErrors)
	}
}

func TestCollector_Seek(t *testing.T) {
	c := NewCollector("test_queue")

	c.RecordSeek()
	c.RecordSeek()
	c.RecordSeek()

	snapshot := c.GetSnapshot()

	if snapshot.SeekOperations != 3 {
		t.Errorf("SeekOperations = %d, want 3", snapshot.SeekOperations)
	}
}

func TestCollector_QueueState(t *testing.T) {
	c := NewCollector("test_queue")

	c.UpdateQueueState(100, 5, 1000, 900)

	snapshot := c.GetSnapshot()

	if snapshot.PendingMessages != 100 {
		t.Errorf("PendingMessages = %d, want 100", snapshot.PendingMessages)
	}

	if snapshot.SegmentCount != 5 {
		t.Errorf("SegmentCount = %d, want 5", snapshot.SegmentCount)
	}

	if snapshot.NextMessageID != 1000 {
		t.Errorf("NextMessageID = %d, want 1000", snapshot.NextMessageID)
	}

	if snapshot.ReadMessageID != 900 {
		t.Errorf("ReadMessageID = %d, want 900", snapshot.ReadMessageID)
	}
}

func TestCollector_Compaction(t *testing.T) {
	c := NewCollector("test_queue")

	c.RecordCompaction(3, 1024*1024, 100*time.Millisecond)
	c.RecordCompaction(2, 512*1024, 50*time.Millisecond)

	snapshot := c.GetSnapshot()

	if snapshot.CompactionsTotal != 2 {
		t.Errorf("CompactionsTotal = %d, want 2", snapshot.CompactionsTotal)
	}

	if snapshot.SegmentsRemoved != 5 {
		t.Errorf("SegmentsRemoved = %d, want 5", snapshot.SegmentsRemoved)
	}

	expectedBytes := uint64(1024*1024 + 512*1024)
	if snapshot.BytesFreed != expectedBytes {
		t.Errorf("BytesFreed = %d, want %d", snapshot.BytesFreed, expectedBytes)
	}

	if snapshot.LastCompactionUnixSec == 0 {
		t.Error("LastCompactionUnixSec should be set")
	}
}

func TestCollector_CompactionErrors(t *testing.T) {
	c := NewCollector("test_queue")

	c.RecordCompactionError()
	c.RecordCompactionError()

	snapshot := c.GetSnapshot()

	if snapshot.CompactionErrors != 2 {
		t.Errorf("CompactionErrors = %d, want 2", snapshot.CompactionErrors)
	}
}

func TestCollector_Reset(t *testing.T) {
	c := NewCollector("test_queue")

	// Record various operations
	c.RecordEnqueue(100, 1*time.Millisecond)
	c.RecordDequeue(100, 1*time.Millisecond)
	c.RecordEnqueueError()
	c.RecordDequeueError()
	c.RecordSeek()
	c.UpdateQueueState(10, 2, 100, 90)
	c.RecordCompaction(1, 1024, 10*time.Millisecond)

	// Reset
	c.Reset()

	snapshot := c.GetSnapshot()

	// Verify everything is zero
	if snapshot.EnqueueTotal != 0 {
		t.Errorf("EnqueueTotal after reset = %d, want 0", snapshot.EnqueueTotal)
	}

	if snapshot.DequeueTotal != 0 {
		t.Errorf("DequeueTotal after reset = %d, want 0", snapshot.DequeueTotal)
	}

	if snapshot.EnqueueErrors != 0 {
		t.Errorf("EnqueueErrors after reset = %d, want 0", snapshot.EnqueueErrors)
	}

	if snapshot.DequeueErrors != 0 {
		t.Errorf("DequeueErrors after reset = %d, want 0", snapshot.DequeueErrors)
	}

	if snapshot.SeekOperations != 0 {
		t.Errorf("SeekOperations after reset = %d, want 0", snapshot.SeekOperations)
	}

	if snapshot.PendingMessages != 0 {
		t.Errorf("PendingMessages after reset = %d, want 0", snapshot.PendingMessages)
	}

	if snapshot.CompactionsTotal != 0 {
		t.Errorf("CompactionsTotal after reset = %d, want 0", snapshot.CompactionsTotal)
	}
}

func TestDurationHistogram_Buckets(t *testing.T) {
	h := newDurationHistogram()

	// Test different duration ranges
	durations := []time.Duration{
		500 * time.Nanosecond,  // bucket 0
		5 * time.Microsecond,   // bucket 1
		50 * time.Microsecond,  // bucket 2
		500 * time.Microsecond, // bucket 3
		5 * time.Millisecond,   // bucket 4
		50 * time.Millisecond,  // bucket 5
		500 * time.Millisecond, // bucket 6
		5 * time.Second,        // bucket 7
	}

	for _, d := range durations {
		h.observe(d)
	}

	// Verify observations were recorded (non-zero buckets)
	var total uint64
	for i := 0; i < 10; i++ {
		total += h.buckets[i].Load()
	}

	if total != uint64(len(durations)) {
		t.Errorf("Total observations = %d, want %d", total, len(durations))
	}
}

func TestDurationHistogram_Percentiles(t *testing.T) {
	h := newDurationHistogram()

	// Add observations in bucket 3 (100μs - 1ms)
	for i := 0; i < 100; i++ {
		h.observe(500 * time.Microsecond)
	}

	// P50 should be in this bucket
	p50 := h.percentile(0.50)
	if p50 == 0 {
		t.Error("P50 should not be zero")
	}

	// P99 should also be in this bucket (all observations are similar)
	p99 := h.percentile(0.99)
	if p99 == 0 {
		t.Error("P99 should not be zero")
	}
}

func TestDurationHistogram_EmptyPercentile(t *testing.T) {
	h := newDurationHistogram()

	// No observations yet
	p50 := h.percentile(0.50)
	if p50 != 0 {
		t.Errorf("P50 on empty histogram = %v, want 0", p50)
	}
}

func TestNoopCollector(t *testing.T) {
	// Verify NoopCollector doesn't panic on any operations
	n := NoopCollector{}

	n.RecordEnqueue(100, 1*time.Millisecond)
	n.RecordDequeue(100, 1*time.Millisecond)
	n.RecordEnqueueBatch(10, 1000, 5*time.Millisecond)
	n.RecordDequeueBatch(5, 500, 3*time.Millisecond)
	n.RecordEnqueueError()
	n.RecordDequeueError()
	n.RecordSeek()
	n.RecordCompaction(3, 1024, 10*time.Millisecond)
	n.RecordCompactionError()
	n.UpdateQueueState(100, 5, 1000, 900)
	n.Reset()

	snapshot := n.GetSnapshot()
	if snapshot != nil {
		t.Error("NoopCollector.GetSnapshot() should return nil")
	}
}

func TestCollector_Concurrent(t *testing.T) {
	c := NewCollector("test_queue")

	// Run concurrent operations
	done := make(chan bool)
	goroutines := 10
	opsPerGoroutine := 100

	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < opsPerGoroutine; j++ {
				c.RecordEnqueue(100, 1*time.Millisecond)
				c.RecordDequeue(100, 1*time.Millisecond)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	snapshot := c.GetSnapshot()

	expectedTotal := uint64(goroutines * opsPerGoroutine)
	if snapshot.EnqueueTotal != expectedTotal {
		t.Errorf("EnqueueTotal = %d, want %d", snapshot.EnqueueTotal, expectedTotal)
	}

	if snapshot.DequeueTotal != expectedTotal {
		t.Errorf("DequeueTotal = %d, want %d", snapshot.DequeueTotal, expectedTotal)
	}
}

func BenchmarkCollector_RecordEnqueue(b *testing.B) {
	c := NewCollector("bench_queue")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.RecordEnqueue(100, 500*time.Microsecond)
	}
}

func BenchmarkCollector_RecordDequeue(b *testing.B) {
	c := NewCollector("bench_queue")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.RecordDequeue(100, 1*time.Millisecond)
	}
}

func BenchmarkCollector_GetSnapshot(b *testing.B) {
	c := NewCollector("bench_queue")

	// Add some data
	for i := 0; i < 1000; i++ {
		c.RecordEnqueue(100, 500*time.Microsecond)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetSnapshot()
	}
}

func BenchmarkCollector_Concurrent(b *testing.B) {
	c := NewCollector("bench_queue")

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.RecordEnqueue(100, 500*time.Microsecond)
		}
	})
}
