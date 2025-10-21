package queue

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDedupTracker_CheckAndTrack(t *testing.T) {
	tracker := NewDedupTracker("", 1000)

	// Check non-existent entry
	offset, isDup := tracker.Check("order-123", 5*time.Minute)
	if isDup {
		t.Error("Expected no duplicate for new entry")
	}
	if offset != 0 {
		t.Errorf("Expected offset=0, got %d", offset)
	}

	// Track new entry
	err := tracker.Track("order-123", 42, 42, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to track entry: %v", err)
	}

	// Check duplicate
	offset, isDup = tracker.Check("order-123", 5*time.Minute)
	if !isDup {
		t.Error("Expected duplicate detection")
	}
	if offset != 42 {
		t.Errorf("Expected offset=42, got %d", offset)
	}

	// Different dedup ID should not be duplicate
	offset, isDup = tracker.Check("order-456", 5*time.Minute)
	if isDup {
		t.Error("Different dedup ID should not be duplicate")
	}
}

func TestDedupTracker_EmptyDedupID(t *testing.T) {
	tracker := NewDedupTracker("", 1000)

	// Check with empty ID
	_, isDup := tracker.Check("", 5*time.Minute)
	if isDup {
		t.Error("Empty dedup ID should not match")
	}

	// Track with empty ID
	err := tracker.Track("", 42, 42, 5*time.Minute)
	if err == nil {
		t.Error("Expected error when tracking empty dedup ID")
	}
}

func TestDedupTracker_Expiration(t *testing.T) {
	tracker := NewDedupTracker("", 1000)

	// Track with very short window
	err := tracker.Track("order-123", 42, 42, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to track: %v", err)
	}

	// Should be duplicate immediately
	_, isDup := tracker.Check("order-123", 10*time.Millisecond)
	if !isDup {
		t.Error("Expected duplicate before expiration")
	}

	// Wait for expiration
	time.Sleep(20 * time.Millisecond)

	// Should no longer be duplicate
	_, isDup = tracker.Check("order-123", 10*time.Millisecond)
	if isDup {
		t.Error("Expected no duplicate after expiration")
	}
}

func TestDedupTracker_CleanExpired(t *testing.T) {
	tracker := NewDedupTracker("", 1000)

	// Track multiple entries with different windows
	tracker.Track("order-1", 1, 1, 10*time.Millisecond)
	tracker.Track("order-2", 2, 2, 1*time.Hour)
	tracker.Track("order-3", 3, 3, 10*time.Millisecond)

	if tracker.Count() != 3 {
		t.Errorf("Expected 3 entries, got %d", tracker.Count())
	}

	// Wait for short windows to expire
	time.Sleep(20 * time.Millisecond)

	// Clean expired
	removed := tracker.CleanExpired()
	if removed != 2 {
		t.Errorf("Expected to remove 2 entries, removed %d", removed)
	}

	if tracker.Count() != 1 {
		t.Errorf("Expected 1 entry remaining, got %d", tracker.Count())
	}

	// Verify the long-lived entry is still there
	_, isDup := tracker.Check("order-2", 1*time.Hour)
	if !isDup {
		t.Error("Expected order-2 to still be tracked")
	}
}

func TestDedupTracker_MaxSize(t *testing.T) {
	tracker := NewDedupTracker("", 5) // Small max size

	// Fill to capacity
	for i := 0; i < 5; i++ {
		err := tracker.Track(string(rune('a'+i)), uint64(i), uint64(i), 1*time.Hour)
		if err != nil {
			t.Fatalf("Failed to track entry %d: %v", i, err)
		}
	}

	// Try to add beyond capacity
	err := tracker.Track("overflow", 999, 999, 1*time.Hour)
	if err == nil {
		t.Error("Expected error when exceeding max size")
	}

	// Clean expired entries (none should expire)
	removed := tracker.CleanExpired()
	if removed != 0 {
		t.Errorf("Expected 0 removed, got %d", removed)
	}

	// Still can't add
	err = tracker.Track("overflow", 999, 999, 1*time.Hour)
	if err == nil {
		t.Error("Expected error after cleanup with no expired entries")
	}
}

func TestDedupTracker_MaxSizeWithExpired(t *testing.T) {
	tracker := NewDedupTracker("", 3)

	// Add entries that will expire
	tracker.Track("temp-1", 1, 1, 10*time.Millisecond)
	tracker.Track("temp-2", 2, 2, 10*time.Millisecond)

	// Add entry that won't expire
	tracker.Track("perm-1", 3, 3, 1*time.Hour)

	// Table is full (3/3)
	err := tracker.Track("new", 4, 4, 1*time.Hour)
	if err == nil {
		t.Error("Expected error when table full")
	}

	// Wait for short-lived entries to expire
	time.Sleep(20 * time.Millisecond)

	// Now we can add new entries (expired ones don't count toward limit)
	err = tracker.Track("new", 4, 4, 1*time.Hour)
	if err != nil {
		t.Errorf("Expected success after entries expired: %v", err)
	}
}

func TestDedupTracker_ActiveCount(t *testing.T) {
	tracker := NewDedupTracker("", 1000)

	// Track some entries with different windows
	tracker.Track("short-1", 1, 1, 10*time.Millisecond)
	tracker.Track("long-1", 2, 2, 1*time.Hour)
	tracker.Track("short-2", 3, 3, 10*time.Millisecond)

	// All should be active initially
	if tracker.ActiveCount() != 3 {
		t.Errorf("Expected 3 active entries, got %d", tracker.ActiveCount())
	}

	// Wait for short windows to expire
	time.Sleep(20 * time.Millisecond)

	// Active count should decrease (but total count stays same until cleanup)
	if tracker.ActiveCount() != 1 {
		t.Errorf("Expected 1 active entry after expiration, got %d", tracker.ActiveCount())
	}
	if tracker.Count() != 3 {
		t.Errorf("Expected 3 total entries before cleanup, got %d", tracker.Count())
	}
}

func TestDedupTracker_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, ".dedup_state.json")

	// Create tracker and add entries
	tracker1 := NewDedupTracker(statePath, 1000)
	tracker1.Track("order-123", 42, 42, 1*time.Hour)
	tracker1.Track("order-456", 99, 99, 1*time.Hour)

	if tracker1.Count() != 2 {
		t.Fatalf("Expected 2 entries, got %d", tracker1.Count())
	}

	// Persist
	err := tracker1.Persist()
	if err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	// Verify state file exists
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		t.Fatal("State file was not created")
	}

	// Create new tracker and load
	tracker2 := NewDedupTracker(statePath, 1000)
	err = tracker2.Load()
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	// Verify entries were restored
	if tracker2.Count() != 2 {
		t.Errorf("Expected 2 entries after load, got %d", tracker2.Count())
	}

	// Verify specific entries
	offset, isDup := tracker2.Check("order-123", 1*time.Hour)
	if !isDup || offset != 42 {
		t.Errorf("Expected order-123 to be tracked with offset=42, got isDup=%v offset=%d", isDup, offset)
	}

	offset, isDup = tracker2.Check("order-456", 1*time.Hour)
	if !isDup || offset != 99 {
		t.Errorf("Expected order-456 to be tracked with offset=99, got isDup=%v offset=%d", isDup, offset)
	}
}

func TestDedupTracker_PersistenceSkipsExpired(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, ".dedup_state.json")

	tracker1 := NewDedupTracker(statePath, 1000)

	// Add entries with different windows
	tracker1.Track("short", 1, 1, 10*time.Millisecond)
	tracker1.Track("long", 2, 2, 1*time.Hour)

	// Wait for short entry to expire
	time.Sleep(20 * time.Millisecond)

	// Persist and load
	tracker1.Persist()
	tracker2 := NewDedupTracker(statePath, 1000)
	tracker2.Load()

	// Only non-expired entry should be loaded
	if tracker2.Count() != 1 {
		t.Errorf("Expected 1 entry after loading (expired skipped), got %d", tracker2.Count())
	}

	// Verify the right one was kept
	_, isDup := tracker2.Check("long", 1*time.Hour)
	if !isDup {
		t.Error("Expected 'long' entry to be present")
	}

	_, isDup = tracker2.Check("short", 1*time.Hour)
	if isDup {
		t.Error("Expected 'short' entry to not be loaded (expired)")
	}
}

func TestDedupTracker_LoadNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "nonexistent.json")

	tracker := NewDedupTracker(statePath, 1000)
	err := tracker.Load()
	if err != nil {
		t.Errorf("Load should succeed with non-existent file: %v", err)
	}

	if tracker.Count() != 0 {
		t.Errorf("Expected empty tracker, got %d entries", tracker.Count())
	}
}

func TestDedupTracker_ConcurrentAccess(t *testing.T) {
	tracker := NewDedupTracker("", 10000)

	// Run concurrent operations
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				dedupID := string(rune('a' + (id*100+j)%26))
				tracker.Track(dedupID, uint64(id*100+j), uint64(id*100+j), 1*time.Hour)
				tracker.Check(dedupID, 1*time.Hour)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should have many entries (exact count depends on dedup ID collisions)
	if tracker.Count() == 0 {
		t.Error("Expected some entries after concurrent operations")
	}
}

func TestDedupTracker_HashCollisionResistance(t *testing.T) {
	tracker := NewDedupTracker("", 1000)

	// Track with one dedup ID
	tracker.Track("order-123", 42, 42, 1*time.Hour)

	// Different dedup ID should not collide
	_, isDup := tracker.Check("order-124", 1*time.Hour)
	if isDup {
		t.Error("Different dedup IDs should not collide")
	}

	// Track multiple similar IDs
	for i := 0; i < 100; i++ {
		id := string(rune('a' + i))
		tracker.Track(id, uint64(i), uint64(i), 1*time.Hour)
	}

	// All should be tracked independently
	for i := 0; i < 100; i++ {
		id := string(rune('a' + i))
		offset, isDup := tracker.Check(id, 1*time.Hour)
		if !isDup {
			t.Errorf("Expected %s to be tracked", id)
		}
		if offset != uint64(i) {
			t.Errorf("Expected offset=%d for %s, got %d", i, id, offset)
		}
	}
}

func TestDedupTracker_Close(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, ".dedup_state.json")

	tracker := NewDedupTracker(statePath, 1000)
	tracker.Track("order-123", 42, 42, 1*time.Hour)

	// Close should persist state
	err := tracker.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify state file was created
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		t.Error("State file was not created by Close")
	}

	// Verify can be loaded
	tracker2 := NewDedupTracker(statePath, 1000)
	tracker2.Load()
	if tracker2.Count() != 1 {
		t.Errorf("Expected 1 entry after Close and Load, got %d", tracker2.Count())
	}
}

// Benchmark tests
func BenchmarkDedupTracker_Check(b *testing.B) {
	tracker := NewDedupTracker("", 100000)
	tracker.Track("order-123", 42, 42, 1*time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.Check("order-123", 1*time.Hour)
	}
}

func BenchmarkDedupTracker_Track(b *testing.B) {
	tracker := NewDedupTracker("", 100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.Track(string(rune('a'+(i%26))), uint64(i), uint64(i), 1*time.Hour)
	}
}

func BenchmarkDedupTracker_CleanExpired(b *testing.B) {
	tracker := NewDedupTracker("", 100000)

	// Add many entries
	for i := 0; i < 10000; i++ {
		tracker.Track(string(rune('a'+(i%26))), uint64(i), uint64(i), 1*time.Hour)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.CleanExpired()
	}
}
