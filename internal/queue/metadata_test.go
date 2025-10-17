package queue

import (
	"os"
	"testing"
)

func TestMetadata_CreateNew(t *testing.T) {
	tmpDir := t.TempDir()

	m, err := OpenMetadata(tmpDir, false)
	if err != nil {
		t.Fatalf("OpenMetadata() error = %v", err)
	}
	defer func() { _ = m.Close() }()

	// Check initial state
	nextMsgID, readMsgID := m.GetState()
	if nextMsgID != 1 {
		t.Errorf("Initial nextMsgID = %d, want 1", nextMsgID)
	}
	if readMsgID != 1 {
		t.Errorf("Initial readMsgID = %d, want 1", readMsgID)
	}
}

func TestMetadata_UpdateAndPersist(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: create and update
	m1, err := OpenMetadata(tmpDir, true)
	if err != nil {
		t.Fatalf("OpenMetadata() first error = %v", err)
	}

	if err := m1.SetNextMsgID(100); err != nil {
		t.Fatalf("SetNextMsgID() error = %v", err)
	}

	if err := m1.SetReadMsgID(50); err != nil {
		t.Fatalf("SetReadMsgID() error = %v", err)
	}

	if err := m1.Close(); err != nil {
		t.Fatalf("Close() first error = %v", err)
	}

	// Second session: verify persistence
	m2, err := OpenMetadata(tmpDir, false)
	if err != nil {
		t.Fatalf("OpenMetadata() second error = %v", err)
	}
	defer func() { _ = m2.Close() }()

	nextMsgID, readMsgID := m2.GetState()
	if nextMsgID != 100 {
		t.Errorf("Persisted nextMsgID = %d, want 100", nextMsgID)
	}
	if readMsgID != 50 {
		t.Errorf("Persisted readMsgID = %d, want 50", readMsgID)
	}
}

func TestMetadata_UpdateState(t *testing.T) {
	tmpDir := t.TempDir()

	m, err := OpenMetadata(tmpDir, true)
	if err != nil {
		t.Fatalf("OpenMetadata() error = %v", err)
	}
	defer func() { _ = m.Close() }()

	// Update both atomically
	if err := m.UpdateState(200, 150); err != nil {
		t.Fatalf("UpdateState() error = %v", err)
	}

	nextMsgID, readMsgID := m.GetState()
	if nextMsgID != 200 {
		t.Errorf("nextMsgID = %d, want 200", nextMsgID)
	}
	if readMsgID != 150 {
		t.Errorf("readMsgID = %d, want 150", readMsgID)
	}
}

func TestMetadata_Sync(t *testing.T) {
	tmpDir := t.TempDir()

	m, err := OpenMetadata(tmpDir, false) // No auto-sync
	if err != nil {
		t.Fatalf("OpenMetadata() error = %v", err)
	}
	defer func() { _ = m.Close() }()

	if err := m.SetNextMsgID(42); err != nil {
		t.Fatalf("SetNextMsgID() error = %v", err)
	}

	// Manual sync
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
}

func TestMetadata_CloseTwice(t *testing.T) {
	tmpDir := t.TempDir()

	m, err := OpenMetadata(tmpDir, false)
	if err != nil {
		t.Fatalf("OpenMetadata() error = %v", err)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close() first error = %v", err)
	}

	// Second close should be no-op
	if err := m.Close(); err != nil {
		t.Errorf("Close() second error = %v, want nil", err)
	}
}

func TestMetadata_CorruptFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a corrupt metadata file
	metaPath := tmpDir + "/metadata.dat"
	if err := os.WriteFile(metaPath, []byte("corrupt"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	// Should fail to open
	_, err := OpenMetadata(tmpDir, false)
	if err == nil {
		t.Error("OpenMetadata() with corrupt file should fail")
	}
}

func TestMetadata_WrongVersion(t *testing.T) {
	tmpDir := t.TempDir()

	// Create metadata with wrong version
	m1, err := OpenMetadata(tmpDir, true)
	if err != nil {
		t.Fatalf("OpenMetadata() error = %v", err)
	}

	// Manually set wrong version and flush
	m1.mu.Lock()
	m1.Version = 999
	_ = m1.flush()
	m1.mu.Unlock()

	if err := m1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Should fail to open with wrong version
	_, err = OpenMetadata(tmpDir, false)
	if err == nil {
		t.Error("OpenMetadata() with wrong version should fail")
	}
}

func TestMetadata_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()

	m, err := OpenMetadata(tmpDir, true)
	if err != nil {
		t.Fatalf("OpenMetadata() error = %v", err)
	}
	defer func() { _ = m.Close() }()

	// Concurrent updates
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 100; i++ {
			_ = m.SetNextMsgID(uint64(i + 1))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			_ = m.SetReadMsgID(uint64(i + 1))
		}
		done <- true
	}()

	<-done
	<-done

	// Should have consistent state
	nextMsgID, readMsgID := m.GetState()
	if nextMsgID == 0 || readMsgID == 0 {
		t.Errorf("Invalid state after concurrent updates: nextMsgID=%d, readMsgID=%d", nextMsgID, readMsgID)
	}
}
