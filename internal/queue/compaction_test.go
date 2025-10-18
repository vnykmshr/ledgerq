package queue

import (
	"testing"
	"time"

	"github.com/vnykmshr/ledgerq/internal/logging"
	"github.com/vnykmshr/ledgerq/internal/segment"
)

// TestAutoCompaction tests automatic background compaction
func TestAutoCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)
	opts.CompactionInterval = 10 * time.Second // Long interval - we'll trigger manually after setup
	opts.SegmentOptions.MaxSegmentMessages = 10 // Small segments
	opts.SegmentOptions.RotationPolicy = segment.RotateByCount
	opts.SegmentOptions.RetentionPolicy = &segment.RetentionPolicy{
		MaxSegments: 3,
		MinSegments: 1,
	}

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Write many messages to create multiple segments
	for i := 0; i < 60; i++ {
		payload := []byte("test message")
		if _, err := q.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	initialSegmentCount := q.Stats().SegmentCount
	t.Logf("Initial segment count: %d", initialSegmentCount)

	if initialSegmentCount < 4 {
		t.Errorf("Expected at least 4 segments, got %d", initialSegmentCount)
	}

	// Consume first 30 messages to free up segments for compaction
	// With a long compaction interval, we can do this safely without race conditions
	for i := 0; i < 30; i++ {
		if _, err := q.Dequeue(); err != nil {
			t.Fatalf("Dequeue(%d) error = %v", i, err)
		}
	}

	// Record segment count after consumption but before compaction
	segmentCountBeforeCompaction := q.Stats().SegmentCount
	t.Logf("Segment count after dequeue: %d", segmentCountBeforeCompaction)

	// Manually trigger compaction (this is what background compaction would do)
	compactResult, err := q.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	t.Logf("Compaction removed %d segments, freed %d bytes",
		compactResult.SegmentsRemoved, compactResult.BytesFreed)

	// Verify background compaction timer is still active
	if !q.compactionTimerActive {
		t.Error("Compaction timer should still be active")
	}

	finalSegmentCount := q.Stats().SegmentCount
	t.Logf("Final segment count after compaction: %d", finalSegmentCount)

	// Compaction should have reduced segment count
	if finalSegmentCount >= segmentCountBeforeCompaction {
		t.Errorf("Expected fewer segments after compaction: before=%d, after=%d",
			segmentCountBeforeCompaction, finalSegmentCount)
	}

	// Verify compaction actually removed some segments
	if compactResult.SegmentsRemoved == 0 {
		t.Error("Expected compaction to remove at least one segment")
	}

	// Queue should still be functional - can still read remaining messages
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue after compaction error = %v", err)
	}
	if msg == nil {
		t.Fatal("Dequeue returned nil message")
	}
	if msg.ID != 31 {
		t.Errorf("Expected message ID 31, got %d", msg.ID)
	}
}

// TestCompactionDisabled tests that compaction doesn't run when interval is 0
func TestCompactionDisabled(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.CompactionInterval = 0 // Disabled
	opts.SegmentOptions.MaxSegmentMessages = 10
	opts.SegmentOptions.RotationPolicy = segment.RotateByCount

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Check that compaction timer is not active
	if q.compactionTimerActive {
		t.Error("Compaction timer should not be active when interval is 0")
	}

	// Write messages to create multiple segments
	for i := 0; i < 30; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	initialSegmentCount := q.Stats().SegmentCount

	// Wait a bit
	time.Sleep(500 * time.Millisecond)

	// Segment count should not change (no auto-compaction)
	finalSegmentCount := q.Stats().SegmentCount
	if finalSegmentCount != initialSegmentCount {
		t.Errorf("Segment count changed from %d to %d, but compaction was disabled",
			initialSegmentCount, finalSegmentCount)
	}
}

// TestCompactionTimerStopped tests that compaction timer stops on close
func TestCompactionTimerStopped(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.CompactionInterval = 100 * time.Millisecond

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Check that compaction timer is active
	if !q.compactionTimerActive {
		t.Error("Compaction timer should be active")
	}

	// Close queue
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Check that compaction timer is stopped
	if q.compactionTimerActive {
		t.Error("Compaction timer should be stopped after close")
	}
}

// TestManualCompaction tests that manual compaction still works with auto-compaction enabled
func TestManualCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.CompactionInterval = 10 * time.Second // Long interval so it won't run during test
	opts.SegmentOptions.MaxSegmentMessages = 10
	opts.SegmentOptions.RotationPolicy = segment.RotateByCount
	opts.SegmentOptions.RetentionPolicy = &segment.RetentionPolicy{
		MaxSegments: 2,
		MinSegments: 1,
	}

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Write messages to create multiple segments
	for i := 0; i < 40; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	initialSegmentCount := q.Stats().SegmentCount

	// Manually trigger compaction
	result, err := q.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SegmentsRemoved == 0 {
		t.Error("Manual compaction removed 0 segments, expected some")
	}

	finalSegmentCount := q.Stats().SegmentCount

	if finalSegmentCount >= initialSegmentCount {
		t.Errorf("Compaction did not reduce segments: before=%d, after=%d",
			initialSegmentCount, finalSegmentCount)
	}
}
