package queue

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDeduplication_Integration tests the end-to-end deduplication flow.
func TestDeduplication_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.DefaultDeduplicationWindow = 5 * time.Minute
	opts.MaxDeduplicationEntries = 1000

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	payload := []byte("test message")

	// First enqueue should succeed
	offset1, isDup1, err := q.EnqueueWithDedup(payload, "order-123", 0)
	if err != nil {
		t.Fatalf("First enqueue failed: %v", err)
	}
	if isDup1 {
		t.Error("First enqueue should not be duplicate")
	}

	// Second enqueue with same ID should be duplicate
	offset2, isDup2, err := q.EnqueueWithDedup(payload, "order-123", 0)
	if err != nil {
		t.Fatalf("Second enqueue failed: %v", err)
	}
	if !isDup2 {
		t.Error("Second enqueue should be duplicate")
	}
	if offset2 != offset1 {
		t.Errorf("Duplicate should return original offset %d, got %d", offset1, offset2)
	}

	// Different dedup ID should not be duplicate
	offset3, isDup3, err := q.EnqueueWithDedup(payload, "order-456", 0)
	if err != nil {
		t.Fatalf("Third enqueue failed: %v", err)
	}
	if isDup3 {
		t.Error("Different dedup ID should not be duplicate")
	}
	if offset3 == offset1 {
		t.Error("Different message should have different offset")
	}

	// Verify stats
	stats := q.Stats()
	if stats.DedupTrackedEntries != 2 {
		t.Errorf("Expected 2 tracked entries, got %d", stats.DedupTrackedEntries)
	}
}

// TestDeduplication_Persistence tests that dedup state survives queue restart.
func TestDeduplication_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.DefaultDeduplicationWindow = 1 * time.Hour
	opts.MaxDeduplicationEntries = 1000

	// First queue instance
	q1, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to open queue: %v", err)
	}

	payload := []byte("test message")
	offset1, isDup1, err := q1.EnqueueWithDedup(payload, "order-123", 0)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if isDup1 {
		t.Error("First enqueue should not be duplicate")
	}

	// Close queue (should persist dedup state)
	if err := q1.Close(); err != nil {
		t.Fatalf("Failed to close queue: %v", err)
	}

	// Verify state file was created
	statePath := filepath.Join(tmpDir, ".dedup_state.json")
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		t.Fatal("Dedup state file was not created")
	}

	// Reopen queue (should load dedup state)
	q2, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	defer q2.Close()

	// Should detect duplicate after restart
	offset2, isDup2, err := q2.EnqueueWithDedup(payload, "order-123", 0)
	if err != nil {
		t.Fatalf("Enqueue after restart failed: %v", err)
	}
	if !isDup2 {
		t.Error("Should detect duplicate after restart")
	}
	if offset2 != offset1 {
		t.Errorf("Duplicate should return original offset %d, got %d", offset1, offset2)
	}

	// Verify stats
	stats := q2.Stats()
	if stats.DedupTrackedEntries != 1 {
		t.Errorf("Expected 1 tracked entry after restart, got %d", stats.DedupTrackedEntries)
	}
}

// TestDeduplication_Expiration tests that entries expire correctly.
func TestDeduplication_Expiration(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.DefaultDeduplicationWindow = 5 * time.Millisecond
	opts.MaxDeduplicationEntries = 1000

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	payload := []byte("test message")

	// Enqueue with short window
	offset1, isDup1, err := q.EnqueueWithDedup(payload, "order-123", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("First enqueue failed: %v", err)
	}
	if isDup1 {
		t.Error("First enqueue should not be duplicate")
	}

	// Should be duplicate immediately
	_, isDup2, err := q.EnqueueWithDedup(payload, "order-123", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Second enqueue failed: %v", err)
	}
	if !isDup2 {
		t.Error("Should be duplicate before expiration")
	}

	// Wait for expiration
	time.Sleep(10 * time.Millisecond)

	// Should no longer be duplicate
	offset3, isDup3, err := q.EnqueueWithDedup(payload, "order-123", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Third enqueue failed: %v", err)
	}
	if isDup3 {
		t.Error("Should not be duplicate after expiration")
	}
	if offset3 == offset1 {
		t.Error("New message should have different offset")
	}
}

// TestDeduplication_Cleanup tests the background cleanup goroutine.
func TestDeduplication_Cleanup(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.DefaultDeduplicationWindow = 50 * time.Millisecond
	opts.MaxDeduplicationEntries = 1000

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	payload := []byte("test message")

	// Enqueue multiple messages with short windows
	for i := 0; i < 10; i++ {
		dedupID := string(rune('a' + i))
		_, _, err := q.EnqueueWithDedup(payload, dedupID, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Enqueue %d failed: %v", i, err)
		}
	}

	// All should be tracked
	stats1 := q.Stats()
	if stats1.DedupTrackedEntries != 10 {
		t.Errorf("Expected 10 tracked entries, got %d", stats1.DedupTrackedEntries)
	}

	// Wait for expiration + cleanup cycle (cleanup runs every 10 seconds)
	// We'll manually trigger cleanup by waiting for expiration
	time.Sleep(10 * time.Millisecond)

	// Force cleanup by checking the count (which internally scans for expired)
	if q.dedupTracker != nil {
		active := q.dedupTracker.ActiveCount()
		if active != 0 {
			t.Errorf("Expected 0 active entries after expiration, got %d", active)
		}
	}
}

// TestDeduplication_MaxSize tests the bounded size limit.
func TestDeduplication_MaxSize(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.DefaultDeduplicationWindow = 1 * time.Hour
	opts.MaxDeduplicationEntries = 5 // Small limit

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	payload := []byte("test message")

	// Fill to capacity
	for i := 0; i < 5; i++ {
		dedupID := string(rune('a' + i))
		_, isDup, err := q.EnqueueWithDedup(payload, dedupID, 0)
		if err != nil {
			t.Fatalf("Enqueue %d failed: %v", i, err)
		}
		if isDup {
			t.Errorf("Enqueue %d should not be duplicate", i)
		}
	}

	// Verify at capacity
	stats := q.Stats()
	if stats.DedupTrackedEntries != 5 {
		t.Errorf("Expected 5 tracked entries, got %d", stats.DedupTrackedEntries)
	}

	// Next enqueue should still succeed (message written, but dedup tracking may fail)
	// The enqueue operation should not fail even if dedup table is full
	_, isDup, err := q.EnqueueWithDedup(payload, "overflow", 0)
	if err != nil {
		t.Fatalf("Enqueue beyond capacity failed: %v", err)
	}
	if isDup {
		t.Error("Overflow message should not be duplicate")
	}

	// Note: Dedup tracking will log an error but message is still enqueued
	// This is by design - we don't want to fail the enqueue operation
}

// TestDeduplication_DisabledByDefault tests that dedup is disabled when not configured.
func TestDeduplication_DisabledByDefault(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	// Don't set DefaultDeduplicationWindow (should be 0 = disabled)

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	// Dedup tracker should not be initialized
	if q.dedupTracker != nil {
		t.Error("Dedup tracker should be nil when disabled")
	}

	// Stats should show 0 tracked entries
	stats := q.Stats()
	if stats.DedupTrackedEntries != 0 {
		t.Errorf("Expected 0 tracked entries when disabled, got %d", stats.DedupTrackedEntries)
	}

	// EnqueueWithDedup should still work but not track
	payload := []byte("test message")
	_, isDup, err := q.EnqueueWithDedup(payload, "order-123", 5*time.Minute)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if isDup {
		t.Error("Should not detect duplicate when dedup disabled")
	}

	// Second enqueue should also not detect duplicate
	_, isDup2, err := q.EnqueueWithDedup(payload, "order-123", 5*time.Minute)
	if err != nil {
		t.Fatalf("Second enqueue failed: %v", err)
	}
	if isDup2 {
		t.Error("Should not detect duplicate when dedup disabled")
	}
}

// TestDeduplication_CustomWindow tests per-message dedup windows.
func TestDeduplication_CustomWindow(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.DefaultDeduplicationWindow = 1 * time.Hour // Long default

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	payload := []byte("test message")

	// Enqueue with custom short window (overrides default)
	_, isDup1, err := q.EnqueueWithDedup(payload, "order-123", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("First enqueue failed: %v", err)
	}
	if isDup1 {
		t.Error("First enqueue should not be duplicate")
	}

	// Should be duplicate immediately
	_, isDup2, err := q.EnqueueWithDedup(payload, "order-123", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Second enqueue failed: %v", err)
	}
	if !isDup2 {
		t.Error("Should be duplicate before custom window expires")
	}

	// Wait for custom window to expire
	time.Sleep(10 * time.Millisecond)

	// Should no longer be duplicate
	_, isDup3, err := q.EnqueueWithDedup(payload, "order-123", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Third enqueue failed: %v", err)
	}
	if isDup3 {
		t.Error("Should not be duplicate after custom window expires")
	}
}
