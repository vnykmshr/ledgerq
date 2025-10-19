package format

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewQueueMetadata(t *testing.T) {
	meta := NewQueueMetadata()

	if meta.Version != CurrentVersion {
		t.Errorf("Version = %d, want %d", meta.Version, CurrentVersion)
	}
	if meta.WriteOffset != 0 {
		t.Errorf("WriteOffset = %d, want 0", meta.WriteOffset)
	}
	if meta.ReadOffset != 0 {
		t.Errorf("ReadOffset = %d, want 0", meta.ReadOffset)
	}
	if meta.AckOffset != 0 {
		t.Errorf("AckOffset = %d, want 0", meta.AckOffset)
	}
	if meta.MessageCount != 0 {
		t.Errorf("MessageCount = %d, want 0", meta.MessageCount)
	}
	if meta.LastWriteTime == 0 {
		t.Error("LastWriteTime should be set to current time")
	}
}

func TestQueueMetadata_Marshal_Unmarshal(t *testing.T) {
	original := &QueueMetadata{
		Version:         1,
		WriteOffset:     1000,
		ReadOffset:      500,
		AckOffset:       400,
		ActiveSegmentID: 5,
		OldestSegmentID: 1,
		MessageCount:    1000,
		AckCount:        400,
		LastWriteTime:   time.Now().UnixNano(),
		LastAckTime:     time.Now().UnixNano(),
		LastCompactTime: time.Now().UnixNano(),
	}

	// Marshal
	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	// Verify it's valid JSON
	if !json.Valid(data) {
		t.Error("Marshal() produced invalid JSON")
	}

	// Unmarshal
	got, err := UnmarshalMetadata(data)
	if err != nil {
		t.Fatalf("UnmarshalMetadata() error = %v", err)
	}

	// Compare fields
	if got.Version != original.Version {
		t.Errorf("Version = %d, want %d", got.Version, original.Version)
	}
	if got.WriteOffset != original.WriteOffset {
		t.Errorf("WriteOffset = %d, want %d", got.WriteOffset, original.WriteOffset)
	}
	if got.ReadOffset != original.ReadOffset {
		t.Errorf("ReadOffset = %d, want %d", got.ReadOffset, original.ReadOffset)
	}
	if got.AckOffset != original.AckOffset {
		t.Errorf("AckOffset = %d, want %d", got.AckOffset, original.AckOffset)
	}
	if got.ActiveSegmentID != original.ActiveSegmentID {
		t.Errorf("ActiveSegmentID = %d, want %d", got.ActiveSegmentID, original.ActiveSegmentID)
	}
	if got.OldestSegmentID != original.OldestSegmentID {
		t.Errorf("OldestSegmentID = %d, want %d", got.OldestSegmentID, original.OldestSegmentID)
	}
	if got.MessageCount != original.MessageCount {
		t.Errorf("MessageCount = %d, want %d", got.MessageCount, original.MessageCount)
	}
	if got.AckCount != original.AckCount {
		t.Errorf("AckCount = %d, want %d", got.AckCount, original.AckCount)
	}
	if got.LastWriteTime != original.LastWriteTime {
		t.Errorf("LastWriteTime = %d, want %d", got.LastWriteTime, original.LastWriteTime)
	}
	if got.LastAckTime != original.LastAckTime {
		t.Errorf("LastAckTime = %d, want %d", got.LastAckTime, original.LastAckTime)
	}
	if got.LastCompactTime != original.LastCompactTime {
		t.Errorf("LastCompactTime = %d, want %d", got.LastCompactTime, original.LastCompactTime)
	}
}

func TestQueueMetadata_Validate(t *testing.T) {
	tests := []struct {
		name    string
		meta    *QueueMetadata
		wantErr bool
	}{
		{
			name: "valid metadata",
			meta: &QueueMetadata{
				Version:         1,
				WriteOffset:     1000,
				ReadOffset:      500,
				AckOffset:       400,
				ActiveSegmentID: 5,
				OldestSegmentID: 1,
				MessageCount:    1000,
				AckCount:        400,
			},
			wantErr: false,
		},
		{
			name: "zero version",
			meta: &QueueMetadata{
				Version: 0,
			},
			wantErr: true,
		},
		{
			name: "unsupported version",
			meta: &QueueMetadata{
				Version: 999,
			},
			wantErr: true,
		},
		{
			name: "read offset > write offset",
			meta: &QueueMetadata{
				Version:     1,
				WriteOffset: 100,
				ReadOffset:  200,
			},
			wantErr: true,
		},
		{
			name: "ack offset > write offset",
			meta: &QueueMetadata{
				Version:     1,
				WriteOffset: 100,
				AckOffset:   200,
			},
			wantErr: true,
		},
		{
			name: "ack count > message count",
			meta: &QueueMetadata{
				Version:      1,
				MessageCount: 100,
				AckCount:     200,
			},
			wantErr: true,
		},
		{
			name: "active segment < oldest segment",
			meta: &QueueMetadata{
				Version:         1,
				ActiveSegmentID: 1,
				OldestSegmentID: 5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.meta.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWriteMetadata_ReadMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	original := &QueueMetadata{
		Version:         1,
		WriteOffset:     1000,
		ReadOffset:      500,
		AckOffset:       400,
		ActiveSegmentID: 5,
		OldestSegmentID: 1,
		MessageCount:    1000,
		AckCount:        400,
		LastWriteTime:   time.Now().UnixNano(),
		LastAckTime:     time.Now().UnixNano(),
	}

	// Write
	if err := WriteMetadata(path, original); err != nil {
		t.Fatalf("WriteMetadata() error = %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("metadata file was not created")
	}

	// Verify .tmp file was cleaned up
	tmpPath := path + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("temporary file was not cleaned up")
	}

	// Read back
	got, err := ReadMetadata(path)
	if err != nil {
		t.Fatalf("ReadMetadata() error = %v", err)
	}

	// Compare
	if got.WriteOffset != original.WriteOffset {
		t.Errorf("WriteOffset = %d, want %d", got.WriteOffset, original.WriteOffset)
	}
	if got.ReadOffset != original.ReadOffset {
		t.Errorf("ReadOffset = %d, want %d", got.ReadOffset, original.ReadOffset)
	}
	if got.MessageCount != original.MessageCount {
		t.Errorf("MessageCount = %d, want %d", got.MessageCount, original.MessageCount)
	}
}

func TestWriteMetadata_AtomicUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	// Write initial version
	v1 := &QueueMetadata{
		Version:      1,
		WriteOffset:  100,
		MessageCount: 100,
	}
	if err := WriteMetadata(path, v1); err != nil {
		t.Fatalf("WriteMetadata() v1 error = %v", err)
	}

	// Update with new version
	v2 := &QueueMetadata{
		Version:      1,
		WriteOffset:  200,
		MessageCount: 200,
	}
	if err := WriteMetadata(path, v2); err != nil {
		t.Fatalf("WriteMetadata() v2 error = %v", err)
	}

	// Read back - should get v2
	got, err := ReadMetadata(path)
	if err != nil {
		t.Fatalf("ReadMetadata() error = %v", err)
	}

	if got.WriteOffset != v2.WriteOffset {
		t.Errorf("WriteOffset = %d, want %d (v2)", got.WriteOffset, v2.WriteOffset)
	}
	if got.MessageCount != v2.MessageCount {
		t.Errorf("MessageCount = %d, want %d (v2)", got.MessageCount, v2.MessageCount)
	}
}

func TestWriteMetadata_InvalidMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	invalid := &QueueMetadata{
		Version:     1,
		WriteOffset: 100,
		ReadOffset:  200, // Invalid: read > write
	}

	err := WriteMetadata(path, invalid)
	if err == nil {
		t.Error("WriteMetadata() should fail with invalid metadata")
	}

	// File should not exist
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("invalid metadata should not create file")
	}
}

func TestReadMetadata_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	// Write invalid JSON
	if err := os.WriteFile(path, []byte("invalid json {"), 0600); err != nil { //nolint:gosec // G306: Test file, restrictive permissions
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := ReadMetadata(path)
	if err == nil {
		t.Error("ReadMetadata() should fail with invalid JSON")
	}
}

func TestReadMetadata_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nonexistent.json")

	_, err := ReadMetadata(path)
	if err == nil {
		t.Error("ReadMetadata() should fail with non-existent file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected IsNotExist error, got: %v", err)
	}
}

func TestReadMetadataOrCreate(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	// First call should create new metadata
	meta1, err := ReadMetadataOrCreate(path)
	if err != nil {
		t.Fatalf("ReadMetadataOrCreate() first call error = %v", err)
	}

	if meta1.Version != CurrentVersion {
		t.Errorf("Version = %d, want %d", meta1.Version, CurrentVersion)
	}

	// Verify file was created
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("metadata file was not created")
	}

	// Second call should read existing metadata
	meta2, err := ReadMetadataOrCreate(path)
	if err != nil {
		t.Fatalf("ReadMetadataOrCreate() second call error = %v", err)
	}

	if meta2.LastWriteTime != meta1.LastWriteTime {
		t.Error("Second call should return same metadata")
	}
}

func TestMetadataFile_ReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	mf := NewMetadataFile(path)

	original := &QueueMetadata{
		Version:      1,
		WriteOffset:  1000,
		ReadOffset:   500,
		MessageCount: 1000,
	}

	// Write
	if err := mf.Write(original); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Read
	got, err := mf.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if got.WriteOffset != original.WriteOffset {
		t.Errorf("WriteOffset = %d, want %d", got.WriteOffset, original.WriteOffset)
	}
	if got.MessageCount != original.MessageCount {
		t.Errorf("MessageCount = %d, want %d", got.MessageCount, original.MessageCount)
	}
}

func TestMetadataFile_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	mf := NewMetadataFile(path)

	// Write initial metadata with non-zero value
	initial := NewQueueMetadata()
	initial.WriteOffset = 1
	initial.MessageCount = 1
	if err := mf.Write(initial); err != nil {
		t.Fatalf("Write() initial error = %v", err)
	}

	// Simulate concurrent updates
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(offset uint64) {
			meta := NewQueueMetadata()
			meta.WriteOffset = offset
			meta.MessageCount = offset
			if err := mf.Write(meta); err != nil {
				t.Errorf("Write() error = %v", err)
			}
			done <- true
		}(uint64((i + 1) * 100)) //nolint:gosec // G115: Test code, safe conversion
	}

	// Wait for all writes
	for i := 0; i < 10; i++ {
		<-done
	}

	// Final read should succeed
	final, err := mf.Read()
	if err != nil {
		t.Fatalf("Read() after concurrent writes error = %v", err)
	}

	// WriteOffset should be one of the written values (initial=1 or 100-1000)
	// Due to concurrent writes, we can't predict which one wins,
	// but it should be a valid value
	validOffsets := map[uint64]bool{1: true}
	for i := 1; i <= 10; i++ {
		validOffsets[uint64(i*100)] = true
	}
	if !validOffsets[final.WriteOffset] {
		t.Errorf("WriteOffset = %d, want one of %v", final.WriteOffset, validOffsets)
	}
}

func TestQueueMetadata_WriteTo_ReadFrom(t *testing.T) {
	original := &QueueMetadata{
		Version:      1,
		WriteOffset:  1000,
		ReadOffset:   500,
		MessageCount: 1000,
	}

	// WriteTo
	buf := new(bytes.Buffer)
	if _, err := original.WriteTo(buf); err != nil {
		t.Fatalf("WriteTo() error = %v", err)
	}

	// ReadFrom
	got, err := ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}

	if got.WriteOffset != original.WriteOffset {
		t.Errorf("WriteOffset = %d, want %d", got.WriteOffset, original.WriteOffset)
	}
	if got.MessageCount != original.MessageCount {
		t.Errorf("MessageCount = %d, want %d", got.MessageCount, original.MessageCount)
	}
}

func BenchmarkQueueMetadata_Marshal(b *testing.B) {
	meta := NewQueueMetadata()
	meta.WriteOffset = 1000000
	meta.MessageCount = 1000000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = meta.Marshal()
	}
}

func BenchmarkQueueMetadata_Unmarshal(b *testing.B) {
	meta := NewQueueMetadata()
	data, _ := meta.Marshal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = UnmarshalMetadata(data)
	}
}

func BenchmarkWriteMetadata(b *testing.B) {
	tmpDir := b.TempDir()
	meta := NewQueueMetadata()
	meta.WriteOffset = 1000000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(tmpDir, "bench.json")
		_ = WriteMetadata(path, meta)
	}
}
