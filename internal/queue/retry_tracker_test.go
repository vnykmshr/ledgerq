package queue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRetryTracker_NewRetryTracker(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	if tracker.maxRetries != 3 {
		t.Errorf("maxRetries = %d, want 3", tracker.maxRetries)
	}

	if tracker.Count() != 0 {
		t.Errorf("Count() = %d, want 0", tracker.Count())
	}
}

func TestRetryTracker_NewRetryTracker_InvalidMaxRetries(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	_, err := NewRetryTracker(path, -1)
	if err == nil {
		t.Error("NewRetryTracker() with negative maxRetries should fail")
	}
}

func TestRetryTracker_Nack_IncrementsCount(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	msgID := uint64(42)

	// First nack
	exceeded, err := tracker.Nack(msgID, "first failure")
	if err != nil {
		t.Fatalf("Nack() error = %v", err)
	}
	if exceeded {
		t.Error("Nack() exceeded = true, want false (first attempt)")
	}

	info := tracker.GetInfo(msgID)
	if info == nil {
		t.Fatal("GetInfo() returned nil")
	}
	if info.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", info.RetryCount)
	}
	if info.FailureReason != "first failure" {
		t.Errorf("FailureReason = %q, want %q", info.FailureReason, "first failure")
	}

	// Second nack
	exceeded, err = tracker.Nack(msgID, "second failure")
	if err != nil {
		t.Fatalf("Nack() error = %v", err)
	}
	if exceeded {
		t.Error("Nack() exceeded = true, want false (second attempt)")
	}

	info = tracker.GetInfo(msgID)
	if info.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", info.RetryCount)
	}

	// Third nack (should exceed)
	exceeded, err = tracker.Nack(msgID, "third failure")
	if err != nil {
		t.Fatalf("Nack() error = %v", err)
	}
	if !exceeded {
		t.Error("Nack() exceeded = false, want true (third attempt with maxRetries=3)")
	}

	info = tracker.GetInfo(msgID)
	if info.RetryCount != 3 {
		t.Errorf("RetryCount = %d, want 3", info.RetryCount)
	}
}

func TestRetryTracker_Nack_UnlimitedRetries(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 0) // 0 = unlimited
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	msgID := uint64(42)

	// Try many nacks - should never exceed
	for i := 0; i < 10; i++ {
		exceeded, err := tracker.Nack(msgID, "failure")
		if err != nil {
			t.Fatalf("Nack() error = %v", err)
		}
		if exceeded {
			t.Errorf("Nack() exceeded = true on attempt %d, want false (unlimited retries)", i+1)
		}
	}

	info := tracker.GetInfo(msgID)
	if info.RetryCount != 10 {
		t.Errorf("RetryCount = %d, want 10", info.RetryCount)
	}
}

func TestRetryTracker_Ack_RemovesEntry(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	msgID := uint64(42)

	// Nack to create entry
	_, err = tracker.Nack(msgID, "failure")
	if err != nil {
		t.Fatalf("Nack() error = %v", err)
	}

	if tracker.Count() != 1 {
		t.Errorf("Count() = %d, want 1", tracker.Count())
	}

	// Ack to remove entry
	err = tracker.Ack(msgID)
	if err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	if tracker.Count() != 0 {
		t.Errorf("Count() = %d, want 0 after Ack", tracker.Count())
	}

	info := tracker.GetInfo(msgID)
	if info != nil {
		t.Error("GetInfo() should return nil after Ack")
	}
}

func TestRetryTracker_Ack_NonExistentMessage(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	// Ack non-existent message should not error
	err = tracker.Ack(uint64(999))
	if err != nil {
		t.Errorf("Ack() on non-existent message error = %v, want nil", err)
	}
}

func TestRetryTracker_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	// Create tracker and add some entries
	tracker1, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}

	msgID1 := uint64(42)
	msgID2 := uint64(43)

	tracker1.Nack(msgID1, "failure 1")
	tracker1.Nack(msgID2, "failure 2")
	tracker1.Nack(msgID2, "failure 2 again")

	tracker1.Close()

	// Reopen and verify state persisted
	tracker2, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() on reload error = %v", err)
	}
	defer tracker2.Close()

	if tracker2.Count() != 2 {
		t.Errorf("Count() after reload = %d, want 2", tracker2.Count())
	}

	info1 := tracker2.GetInfo(msgID1)
	if info1 == nil {
		t.Fatal("GetInfo(msgID1) returned nil after reload")
	}
	if info1.RetryCount != 1 {
		t.Errorf("msgID1 RetryCount = %d, want 1", info1.RetryCount)
	}

	info2 := tracker2.GetInfo(msgID2)
	if info2 == nil {
		t.Fatal("GetInfo(msgID2) returned nil after reload")
	}
	if info2.RetryCount != 2 {
		t.Errorf("msgID2 RetryCount = %d, want 2", info2.RetryCount)
	}
}

func TestRetryTracker_GetInfo_ReturnsNilForNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	info := tracker.GetInfo(uint64(999))
	if info != nil {
		t.Error("GetInfo() for non-existent message should return nil")
	}
}

func TestRetryTracker_GetInfo_ReturnsCopy(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	msgID := uint64(42)
	tracker.Nack(msgID, "original failure")

	// Get info and modify it
	info := tracker.GetInfo(msgID)
	info.RetryCount = 999
	info.FailureReason = "modified"

	// Original should be unchanged
	info2 := tracker.GetInfo(msgID)
	if info2.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1 (original should be unchanged)", info2.RetryCount)
	}
	if info2.FailureReason != "original failure" {
		t.Errorf("FailureReason = %q, want %q", info2.FailureReason, "original failure")
	}
}

func TestRetryTracker_LastFailureTimestamp(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	msgID := uint64(42)

	before := time.Now()
	tracker.Nack(msgID, "failure")
	after := time.Now()

	info := tracker.GetInfo(msgID)
	if info.LastFailure.Before(before) || info.LastFailure.After(after) {
		t.Errorf("LastFailure timestamp %v not in expected range [%v, %v]",
			info.LastFailure, before, after)
	}
}

func TestRetryTracker_Clear(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	// Add some entries
	tracker.Nack(uint64(1), "failure 1")
	tracker.Nack(uint64(2), "failure 2")
	tracker.Nack(uint64(3), "failure 3")

	if tracker.Count() != 3 {
		t.Errorf("Count() = %d, want 3", tracker.Count())
	}

	// Clear
	err = tracker.Clear()
	if err != nil {
		t.Fatalf("Clear() error = %v", err)
	}

	if tracker.Count() != 0 {
		t.Errorf("Count() after Clear() = %d, want 0", tracker.Count())
	}

	// Verify persistence
	tracker.Close()

	tracker2, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker2.Close()

	if tracker2.Count() != 0 {
		t.Errorf("Count() after reload = %d, want 0 (Clear should persist)", tracker2.Count())
	}
}

func TestRetryTracker_MultipleMessages(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	// Track multiple messages with different retry counts
	messages := []struct {
		id      uint64
		retries int
	}{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 1},
		{5, 2},
	}

	for _, msg := range messages {
		for i := 0; i < msg.retries; i++ {
			tracker.Nack(msg.id, "failure")
		}
	}

	if tracker.Count() != 5 {
		t.Errorf("Count() = %d, want 5", tracker.Count())
	}

	// Verify counts
	for _, msg := range messages {
		info := tracker.GetInfo(msg.id)
		if info == nil {
			t.Errorf("GetInfo(%d) returned nil", msg.id)
			continue
		}
		if info.RetryCount != msg.retries {
			t.Errorf("Message %d: RetryCount = %d, want %d",
				msg.id, info.RetryCount, msg.retries)
		}
	}

	// Ack some messages
	tracker.Ack(uint64(2))
	tracker.Ack(uint64(4))

	if tracker.Count() != 3 {
		t.Errorf("Count() after Ack = %d, want 3", tracker.Count())
	}

	// Verify remaining messages
	remaining := []uint64{1, 3, 5}
	for _, id := range remaining {
		info := tracker.GetInfo(id)
		if info == nil {
			t.Errorf("GetInfo(%d) should not be nil", id)
		}
	}

	// Verify removed messages
	removed := []uint64{2, 4}
	for _, id := range removed {
		info := tracker.GetInfo(id)
		if info != nil {
			t.Errorf("GetInfo(%d) should be nil after Ack", id)
		}
	}
}

func TestRetryTracker_CorruptedFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	// Write corrupted JSON
	err := os.WriteFile(path, []byte("not valid json"), 0644)
	if err != nil {
		t.Fatalf("Failed to write corrupted file: %v", err)
	}

	// Should fail to load
	_, err = NewRetryTracker(path, 3)
	if err == nil {
		t.Error("NewRetryTracker() should fail with corrupted file")
	}
}

func TestRetryTracker_WrongVersion(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	// Write state with wrong version
	wrongVersion := retryState{
		Version:    999,
		MaxRetries: 3,
		Entries:    []*RetryInfo{},
	}
	data, _ := json.Marshal(wrongVersion)
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Should fail to load
	_, err = NewRetryTracker(path, 3)
	if err == nil {
		t.Error("NewRetryTracker() should fail with wrong version")
	}
}

func TestRetryTracker_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 10)
	if err != nil {
		t.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	// Concurrent nacks and acks
	done := make(chan bool)
	numGoroutines := 10
	opsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			msgID := uint64(id)
			for j := 0; j < opsPerGoroutine; j++ {
				if j%2 == 0 {
					tracker.Nack(msgID, "concurrent failure")
				} else {
					tracker.GetInfo(msgID)
				}
			}
			done <- true
		}(i)
	}

	// Wait for completion
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify state is consistent (all messages tracked)
	if tracker.Count() != numGoroutines {
		t.Errorf("Count() = %d, want %d", tracker.Count(), numGoroutines)
	}
}

func BenchmarkRetryTracker_Nack(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		b.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.Nack(uint64(i), "benchmark failure")
	}
}

func BenchmarkRetryTracker_Ack(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		b.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	// Pre-populate with entries
	for i := 0; i < b.N; i++ {
		tracker.Nack(uint64(i), "failure")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.Ack(uint64(i))
	}
}

func BenchmarkRetryTracker_GetInfo(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "retry-state.json")

	tracker, err := NewRetryTracker(path, 3)
	if err != nil {
		b.Fatalf("NewRetryTracker() error = %v", err)
	}
	defer tracker.Close()

	// Pre-populate
	tracker.Nack(uint64(42), "failure")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.GetInfo(uint64(42))
	}
}
