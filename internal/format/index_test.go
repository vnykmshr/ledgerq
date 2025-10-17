package format

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func TestIndexHeader_Marshal_Unmarshal_Roundtrip(t *testing.T) {
	header := &IndexHeader{
		Magic:      IndexMagic,
		Version:    CurrentVersion,
		BaseID:     1000,
		EntryCount: 42,
		Reserved:   0,
	}

	// Marshal
	data := header.Marshal()

	// Verify size
	if len(data) != IndexHeaderSize {
		t.Errorf("marshaled size = %d, want %d", len(data), IndexHeaderSize)
	}

	// Unmarshal
	reader := bytes.NewReader(data)
	got, err := UnmarshalIndexHeader(reader)
	if err != nil {
		t.Fatalf("UnmarshalIndexHeader() error = %v", err)
	}

	// Compare fields
	if got.Magic != header.Magic {
		t.Errorf("Magic = %08x, want %08x", got.Magic, header.Magic)
	}
	if got.Version != header.Version {
		t.Errorf("Version = %d, want %d", got.Version, header.Version)
	}
	if got.BaseID != header.BaseID {
		t.Errorf("BaseID = %d, want %d", got.BaseID, header.BaseID)
	}
	if got.EntryCount != header.EntryCount {
		t.Errorf("EntryCount = %d, want %d", got.EntryCount, header.EntryCount)
	}
}

func TestIndexEntry_Marshal_Unmarshal_Roundtrip(t *testing.T) {
	entry := &IndexEntry{
		MessageID:  12345,
		FileOffset: 67890,
		Timestamp:  time.Now().UnixNano(),
	}

	// Marshal
	data := entry.Marshal()

	// Verify size
	if len(data) != IndexEntrySize {
		t.Errorf("marshaled size = %d, want %d", len(data), IndexEntrySize)
	}

	// Unmarshal
	reader := bytes.NewReader(data)
	got, err := UnmarshalIndexEntry(reader)
	if err != nil {
		t.Fatalf("UnmarshalIndexEntry() error = %v", err)
	}

	// Compare fields
	if got.MessageID != entry.MessageID {
		t.Errorf("MessageID = %d, want %d", got.MessageID, entry.MessageID)
	}
	if got.FileOffset != entry.FileOffset {
		t.Errorf("FileOffset = %d, want %d", got.FileOffset, entry.FileOffset)
	}
	if got.Timestamp != entry.Timestamp {
		t.Errorf("Timestamp = %d, want %d", got.Timestamp, entry.Timestamp)
	}
}

func TestNewIndex(t *testing.T) {
	baseID := uint64(1000)
	idx := NewIndex(baseID)

	if idx.Header.Magic != IndexMagic {
		t.Errorf("Magic = %08x, want %08x", idx.Header.Magic, IndexMagic)
	}
	if idx.Header.BaseID != baseID {
		t.Errorf("BaseID = %d, want %d", idx.Header.BaseID, baseID)
	}
	if idx.Header.EntryCount != 0 {
		t.Errorf("EntryCount = %d, want 0", idx.Header.EntryCount)
	}
	if len(idx.Entries) != 0 {
		t.Errorf("Entries length = %d, want 0", len(idx.Entries))
	}
}

func TestIndex_Add(t *testing.T) {
	idx := NewIndex(1000)

	entries := []*IndexEntry{
		{MessageID: 1000, FileOffset: 100, Timestamp: 1000},
		{MessageID: 2000, FileOffset: 200, Timestamp: 2000},
		{MessageID: 3000, FileOffset: 300, Timestamp: 3000},
	}

	for _, e := range entries {
		idx.Add(e)
	}

	if idx.Header.EntryCount != uint64(len(entries)) {
		t.Errorf("EntryCount = %d, want %d", idx.Header.EntryCount, len(entries))
	}
	if len(idx.Entries) != len(entries) {
		t.Errorf("Entries length = %d, want %d", len(idx.Entries), len(entries))
	}
}

func TestIndex_Find(t *testing.T) {
	idx := NewIndex(1000)
	idx.Add(&IndexEntry{MessageID: 1000, FileOffset: 100, Timestamp: 1000})
	idx.Add(&IndexEntry{MessageID: 2000, FileOffset: 200, Timestamp: 2000})
	idx.Add(&IndexEntry{MessageID: 3000, FileOffset: 300, Timestamp: 3000})
	idx.Add(&IndexEntry{MessageID: 4000, FileOffset: 400, Timestamp: 4000})

	tests := []struct {
		name      string
		messageID uint64
		wantID    uint64
		wantNil   bool
	}{
		{"exact match first", 1000, 1000, false},
		{"exact match middle", 2000, 2000, false},
		{"exact match last", 4000, 4000, false},
		{"between entries", 2500, 2000, false},
		{"before first", 500, 0, true},
		{"after last", 5000, 4000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := idx.Find(tt.messageID)
			if tt.wantNil {
				if got != nil {
					t.Errorf("Find(%d) = %v, want nil", tt.messageID, got)
				}
			} else {
				if got == nil {
					t.Fatalf("Find(%d) = nil, want entry", tt.messageID)
				}
				if got.MessageID != tt.wantID {
					t.Errorf("Find(%d) returned MessageID = %d, want %d", tt.messageID, got.MessageID, tt.wantID)
				}
			}
		})
	}
}

func TestIndex_Find_EmptyIndex(t *testing.T) {
	idx := NewIndex(1000)
	got := idx.Find(1000)
	if got != nil {
		t.Errorf("Find on empty index should return nil, got %v", got)
	}
}

func TestIndex_WriteTo_ReadFrom(t *testing.T) {
	// Create index
	original := NewIndex(1000)
	original.Add(&IndexEntry{MessageID: 1000, FileOffset: 100, Timestamp: 1000})
	original.Add(&IndexEntry{MessageID: 2000, FileOffset: 200, Timestamp: 2000})
	original.Add(&IndexEntry{MessageID: 3000, FileOffset: 300, Timestamp: 3000})

	// Write to buffer
	buf := new(bytes.Buffer)
	if _, err := original.WriteTo(buf); err != nil {
		t.Fatalf("WriteTo() error = %v", err)
	}

	// Read back
	got, err := ReadIndex(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadIndex() error = %v", err)
	}

	// Compare
	if got.Header.BaseID != original.Header.BaseID {
		t.Errorf("BaseID = %d, want %d", got.Header.BaseID, original.Header.BaseID)
	}
	if got.Header.EntryCount != original.Header.EntryCount {
		t.Errorf("EntryCount = %d, want %d", got.Header.EntryCount, original.Header.EntryCount)
	}
	if len(got.Entries) != len(original.Entries) {
		t.Fatalf("Entries length = %d, want %d", len(got.Entries), len(original.Entries))
	}

	for i := range got.Entries {
		if got.Entries[i].MessageID != original.Entries[i].MessageID {
			t.Errorf("Entry[%d].MessageID = %d, want %d", i, got.Entries[i].MessageID, original.Entries[i].MessageID)
		}
		if got.Entries[i].FileOffset != original.Entries[i].FileOffset {
			t.Errorf("Entry[%d].FileOffset = %d, want %d", i, got.Entries[i].FileOffset, original.Entries[i].FileOffset)
		}
	}
}

func TestIndexFile_WriteRead(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.idx")

	// Create and write index
	original := NewIndex(1000)
	original.Add(&IndexEntry{MessageID: 1000, FileOffset: 100, Timestamp: time.Now().UnixNano()})
	original.Add(&IndexEntry{MessageID: 2000, FileOffset: 200, Timestamp: time.Now().UnixNano()})

	if err := WriteIndexFile(path, original); err != nil {
		t.Fatalf("WriteIndexFile() error = %v", err)
	}

	// Read back
	got, err := ReadIndexFile(path)
	if err != nil {
		t.Fatalf("ReadIndexFile() error = %v", err)
	}

	// Compare
	if got.Header.BaseID != original.Header.BaseID {
		t.Errorf("BaseID = %d, want %d", got.Header.BaseID, original.Header.BaseID)
	}
	if len(got.Entries) != len(original.Entries) {
		t.Errorf("Entries length = %d, want %d", len(got.Entries), len(original.Entries))
	}
}

func TestIndexHeader_Validate(t *testing.T) {
	tests := []struct {
		name    string
		header  *IndexHeader
		wantErr bool
	}{
		{
			name: "valid header",
			header: &IndexHeader{
				Magic:   IndexMagic,
				Version: CurrentVersion,
				BaseID:  1000,
			},
			wantErr: false,
		},
		{
			name: "invalid magic",
			header: &IndexHeader{
				Magic:   0xDEADBEEF,
				Version: CurrentVersion,
			},
			wantErr: true,
		},
		{
			name: "zero version",
			header: &IndexHeader{
				Magic:   IndexMagic,
				Version: 0,
			},
			wantErr: true,
		},
		{
			name: "unsupported version",
			header: &IndexHeader{
				Magic:   IndexMagic,
				Version: 999,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.header.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func BenchmarkIndexEntry_Marshal(b *testing.B) {
	entry := &IndexEntry{
		MessageID:  12345,
		FileOffset: 67890,
		Timestamp:  time.Now().UnixNano(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = entry.Marshal()
	}
}

func BenchmarkIndex_Find(b *testing.B) {
	idx := NewIndex(1000)
	for i := uint64(0); i < 10000; i++ {
		idx.Add(&IndexEntry{
			MessageID:  1000 + i*100,
			FileOffset: i * 1000,
			Timestamp:  int64(i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Find(500000)
	}
}
