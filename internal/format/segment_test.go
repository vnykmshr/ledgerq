package format

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSegmentHeader_Marshal_Unmarshal_Roundtrip(t *testing.T) {
	tests := []struct {
		name   string
		header *SegmentHeader
	}{
		{
			name: "new segment",
			header: &SegmentHeader{
				Magic:      SegmentMagic,
				Version:    CurrentVersion,
				Flags:      SegmentFlagNone,
				BaseOffset: 1000,
				CreatedAt:  time.Now().UnixNano(),
				MinMsgID:   1000,
				MaxMsgID:   2000,
			},
		},
		{
			name: "segment with compression flag",
			header: &SegmentHeader{
				Magic:      SegmentMagic,
				Version:    CurrentVersion,
				Flags:      SegmentFlagCompressed,
				BaseOffset: 5000,
				CreatedAt:  time.Now().UnixNano(),
				MinMsgID:   5000,
				MaxMsgID:   6000,
			},
		},
		{
			name: "empty segment",
			header: &SegmentHeader{
				Magic:      SegmentMagic,
				Version:    CurrentVersion,
				Flags:      SegmentFlagNone,
				BaseOffset: 0,
				CreatedAt:  time.Now().UnixNano(),
				MinMsgID:   0,
				MaxMsgID:   0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data := tt.header.Marshal()

			// Verify size
			if len(data) != SegmentHeaderSize {
				t.Errorf("marshaled size = %d, want %d", len(data), SegmentHeaderSize)
			}

			// Unmarshal
			reader := bytes.NewReader(data)
			got, err := UnmarshalSegmentHeader(reader)
			if err != nil {
				t.Fatalf("UnmarshalSegmentHeader() error = %v", err)
			}

			// Compare fields
			if got.Magic != tt.header.Magic {
				t.Errorf("Magic = %08x, want %08x", got.Magic, tt.header.Magic)
			}
			if got.Version != tt.header.Version {
				t.Errorf("Version = %d, want %d", got.Version, tt.header.Version)
			}
			if got.Flags != tt.header.Flags {
				t.Errorf("Flags = %d, want %d", got.Flags, tt.header.Flags)
			}
			if got.BaseOffset != tt.header.BaseOffset {
				t.Errorf("BaseOffset = %d, want %d", got.BaseOffset, tt.header.BaseOffset)
			}
			if got.CreatedAt != tt.header.CreatedAt {
				t.Errorf("CreatedAt = %d, want %d", got.CreatedAt, tt.header.CreatedAt)
			}
			if got.MinMsgID != tt.header.MinMsgID {
				t.Errorf("MinMsgID = %d, want %d", got.MinMsgID, tt.header.MinMsgID)
			}
			if got.MaxMsgID != tt.header.MaxMsgID {
				t.Errorf("MaxMsgID = %d, want %d", got.MaxMsgID, tt.header.MaxMsgID)
			}
		})
	}
}

func TestSegmentHeader_UnmarshalCorruptedCRC(t *testing.T) {
	header := &SegmentHeader{
		Magic:      SegmentMagic,
		Version:    CurrentVersion,
		Flags:      SegmentFlagNone,
		BaseOffset: 1000,
		CreatedAt:  time.Now().UnixNano(),
		MinMsgID:   1000,
		MaxMsgID:   2000,
	}

	data := header.Marshal()

	// Corrupt a field (BaseOffset) to cause CRC mismatch
	data[10] ^= 0xFF

	reader := bytes.NewReader(data)
	_, err := UnmarshalSegmentHeader(reader)
	if err == nil {
		t.Error("UnmarshalSegmentHeader() should fail with corrupted data")
	}
	if err != nil && !bytes.Contains([]byte(err.Error()), []byte("CRC mismatch")) {
		t.Errorf("expected CRC mismatch error, got: %v", err)
	}
}

func TestSegmentHeader_UnmarshalTruncated(t *testing.T) {
	header := &SegmentHeader{
		Magic:      SegmentMagic,
		Version:    CurrentVersion,
		Flags:      SegmentFlagNone,
		BaseOffset: 1000,
		CreatedAt:  time.Now().UnixNano(),
	}

	data := header.Marshal()

	// Truncate data
	truncated := data[:32]

	reader := bytes.NewReader(truncated)
	_, err := UnmarshalSegmentHeader(reader)
	if err == nil {
		t.Error("UnmarshalSegmentHeader() should fail with truncated data")
	}
}

func TestNewSegmentHeader(t *testing.T) {
	baseOffset := uint64(5000)
	header := NewSegmentHeader(baseOffset)

	if header.Magic != SegmentMagic {
		t.Errorf("Magic = %08x, want %08x", header.Magic, SegmentMagic)
	}
	if header.Version != CurrentVersion {
		t.Errorf("Version = %d, want %d", header.Version, CurrentVersion)
	}
	if header.BaseOffset != baseOffset {
		t.Errorf("BaseOffset = %d, want %d", header.BaseOffset, baseOffset)
	}
	if header.Flags != SegmentFlagNone {
		t.Errorf("Flags = %d, want %d", header.Flags, SegmentFlagNone)
	}
}

func TestSegmentHeader_Validate(t *testing.T) {
	tests := []struct {
		name    string
		header  *SegmentHeader
		wantErr bool
	}{
		{
			name: "valid header",
			header: &SegmentHeader{
				Magic:      SegmentMagic,
				Version:    CurrentVersion,
				Flags:      SegmentFlagNone,
				BaseOffset: 1000,
				MinMsgID:   1000,
				MaxMsgID:   2000,
			},
			wantErr: false,
		},
		{
			name: "invalid magic",
			header: &SegmentHeader{
				Magic:      0xDEADBEEF,
				Version:    CurrentVersion,
				BaseOffset: 1000,
			},
			wantErr: true,
		},
		{
			name: "zero version",
			header: &SegmentHeader{
				Magic:      SegmentMagic,
				Version:    0,
				BaseOffset: 1000,
			},
			wantErr: true,
		},
		{
			name: "unsupported version",
			header: &SegmentHeader{
				Magic:      SegmentMagic,
				Version:    999,
				BaseOffset: 1000,
			},
			wantErr: true,
		},
		{
			name: "invalid message ID range",
			header: &SegmentHeader{
				Magic:      SegmentMagic,
				Version:    CurrentVersion,
				BaseOffset: 1000,
				MinMsgID:   2000,
				MaxMsgID:   1000,
			},
			wantErr: true,
		},
		{
			name: "zero max ID is valid",
			header: &SegmentHeader{
				Magic:      SegmentMagic,
				Version:    CurrentVersion,
				BaseOffset: 1000,
				MinMsgID:   1000,
				MaxMsgID:   0,
			},
			wantErr: false,
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

func TestSegmentHeader_ReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "segment-001.log")

	// Create and write header
	original := &SegmentHeader{
		Magic:      SegmentMagic,
		Version:    CurrentVersion,
		Flags:      SegmentFlagNone,
		BaseOffset: 1000,
		CreatedAt:  time.Now().UnixNano(),
		MinMsgID:   1000,
		MaxMsgID:   2000,
	}

	if err := WriteSegmentHeader(path, original); err != nil {
		t.Fatalf("WriteSegmentHeader() error = %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("segment file was not created")
	}

	// Read header back
	got, err := ReadSegmentHeader(path)
	if err != nil {
		t.Fatalf("ReadSegmentHeader() error = %v", err)
	}

	// Compare
	if got.Magic != original.Magic {
		t.Errorf("Magic = %08x, want %08x", got.Magic, original.Magic)
	}
	if got.BaseOffset != original.BaseOffset {
		t.Errorf("BaseOffset = %d, want %d", got.BaseOffset, original.BaseOffset)
	}
	if got.MinMsgID != original.MinMsgID {
		t.Errorf("MinMsgID = %d, want %d", got.MinMsgID, original.MinMsgID)
	}
	if got.MaxMsgID != original.MaxMsgID {
		t.Errorf("MaxMsgID = %d, want %d", got.MaxMsgID, original.MaxMsgID)
	}
}

func BenchmarkSegmentHeader_Marshal(b *testing.B) {
	header := NewSegmentHeader(1000)
	header.CreatedAt = time.Now().UnixNano()
	header.MinMsgID = 1000
	header.MaxMsgID = 2000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = header.Marshal()
	}
}

func BenchmarkSegmentHeader_Unmarshal(b *testing.B) {
	header := NewSegmentHeader(1000)
	header.CreatedAt = time.Now().UnixNano()
	data := header.Marshal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, _ = UnmarshalSegmentHeader(reader)
	}
}
