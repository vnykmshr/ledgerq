package segment

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFormatSegmentName(t *testing.T) {
	tests := []struct {
		name       string
		baseOffset uint64
		want       string
	}{
		{"zero offset", 0, "00000000000000000000.log"},
		{"small offset", 1000, "00000000000000001000.log"},
		{"large offset", 1234567890123456789, "01234567890123456789.log"},
		{"max uint64", ^uint64(0), "18446744073709551615.log"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatSegmentName(tt.baseOffset)
			if got != tt.want {
				t.Errorf("FormatSegmentName(%d) = %s, want %s", tt.baseOffset, got, tt.want)
			}
		})
	}
}

func TestFormatIndexName(t *testing.T) {
	tests := []struct {
		name       string
		baseOffset uint64
		want       string
	}{
		{"zero offset", 0, "00000000000000000000.idx"},
		{"small offset", 1000, "00000000000000001000.idx"},
		{"large offset", 1234567890123456789, "01234567890123456789.idx"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatIndexName(tt.baseOffset)
			if got != tt.want {
				t.Errorf("FormatIndexName(%d) = %s, want %s", tt.baseOffset, got, tt.want)
			}
		})
	}
}

func TestParseSegmentName(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     uint64
		wantErr  bool
	}{
		{"valid zero", "00000000000000000000.log", 0, false},
		{"valid small", "00000000000000001000.log", 1000, false},
		{"valid large", "01234567890123456789.log", 1234567890123456789, false},
		{"missing extension", "00000000000000001000", 0, true},
		{"wrong extension", "00000000000000001000.txt", 0, true},
		{"invalid number", "abcdefghijklmnopqrst.log", 0, true},
		{"empty", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSegmentName(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSegmentName(%s) error = %v, wantErr %v", tt.filename, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseSegmentName(%s) = %d, want %d", tt.filename, got, tt.want)
			}
		})
	}
}

func TestParseIndexName(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     uint64
		wantErr  bool
	}{
		{"valid zero", "00000000000000000000.idx", 0, false},
		{"valid small", "00000000000000001000.idx", 1000, false},
		{"missing extension", "00000000000000001000", 0, true},
		{"wrong extension", "00000000000000001000.log", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseIndexName(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseIndexName(%s) error = %v, wantErr %v", tt.filename, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseIndexName(%s) = %d, want %d", tt.filename, got, tt.want)
			}
		})
	}
}

func TestFormatParse_Roundtrip(t *testing.T) {
	offsets := []uint64{0, 1, 1000, 999999, 1234567890123456789}

	for _, offset := range offsets {
		// Test segment name
		segName := FormatSegmentName(offset)
		gotOffset, err := ParseSegmentName(segName)
		if err != nil {
			t.Errorf("ParseSegmentName(%s) error = %v", segName, err)
		}
		if gotOffset != offset {
			t.Errorf("roundtrip segment: got %d, want %d", gotOffset, offset)
		}

		// Test index name
		idxName := FormatIndexName(offset)
		gotOffset, err = ParseIndexName(idxName)
		if err != nil {
			t.Errorf("ParseIndexName(%s) error = %v", idxName, err)
		}
		if gotOffset != offset {
			t.Errorf("roundtrip index: got %d, want %d", gotOffset, offset)
		}
	}
}

func TestDiscoverSegments(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test segment files
	segments := []uint64{0, 1000, 2000, 5000}
	for _, offset := range segments {
		segPath := filepath.Join(tmpDir, FormatSegmentName(offset))
		if err := os.WriteFile(segPath, []byte("test data"), 0644); err != nil {
			t.Fatalf("failed to create test segment: %v", err)
		}

		// Create index for some segments
		if offset != 5000 { // Skip index for last one to test missing index handling
			idxPath := filepath.Join(tmpDir, FormatIndexName(offset))
			if err := os.WriteFile(idxPath, []byte("test index"), 0644); err != nil {
				t.Fatalf("failed to create test index: %v", err)
			}
		}
	}

	// Create some non-segment files (should be ignored)
	os.WriteFile(filepath.Join(tmpDir, "metadata.json"), []byte("{}"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "invalid.txt"), []byte("data"), 0644)

	// Discover segments
	found, err := DiscoverSegments(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverSegments() error = %v", err)
	}

	// Verify count
	if len(found) != len(segments) {
		t.Errorf("found %d segments, want %d", len(found), len(segments))
	}

	// Verify sorting and content
	for i, seg := range found {
		expectedOffset := segments[i]
		if seg.BaseOffset != expectedOffset {
			t.Errorf("segment[%d].BaseOffset = %d, want %d", i, seg.BaseOffset, expectedOffset)
		}

		expectedPath := filepath.Join(tmpDir, FormatSegmentName(expectedOffset))
		if seg.Path != expectedPath {
			t.Errorf("segment[%d].Path = %s, want %s", i, seg.Path, expectedPath)
		}

		if seg.Size != 9 { // "test data" = 9 bytes
			t.Errorf("segment[%d].Size = %d, want 9", i, seg.Size)
		}
	}
}

func TestDiscoverSegments_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	found, err := DiscoverSegments(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverSegments() error = %v", err)
	}

	if len(found) != 0 {
		t.Errorf("found %d segments in empty dir, want 0", len(found))
	}
}

func TestDiscoverSegments_NonExistent(t *testing.T) {
	_, err := DiscoverSegments("/nonexistent/directory")
	if err == nil {
		t.Error("DiscoverSegments() should fail for non-existent directory")
	}
}

func TestValidateSegmentSequence(t *testing.T) {
	tests := []struct {
		name     string
		segments []*SegmentInfo
		wantErr  bool
	}{
		{
			name:     "empty",
			segments: []*SegmentInfo{},
			wantErr:  false,
		},
		{
			name: "valid sequence",
			segments: []*SegmentInfo{
				{BaseOffset: 0},
				{BaseOffset: 1000},
				{BaseOffset: 2000},
			},
			wantErr: false,
		},
		{
			name: "duplicate offset",
			segments: []*SegmentInfo{
				{BaseOffset: 1000},
				{BaseOffset: 1000},
			},
			wantErr: true,
		},
		{
			name: "single segment",
			segments: []*SegmentInfo{
				{BaseOffset: 1000},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSegmentSequence(tt.segments)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSegmentSequence() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSegmentNameWidth(t *testing.T) {
	// Verify that formatted names have consistent width
	names := []string{
		FormatSegmentName(0),
		FormatSegmentName(1),
		FormatSegmentName(999999),
		FormatSegmentName(1234567890123456789),
	}

	expectedLen := SegmentNameWidth + len(SegmentFileExtension) // 20 digits + ".log"
	for _, name := range names {
		if len(name) != expectedLen {
			t.Errorf("segment name %s has length %d, want %d", name, len(name), expectedLen)
		}
	}
}
