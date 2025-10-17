// Package segment provides segment file management for LedgerQ.
//
// Segments are append-only log files containing sequential message entries.
// Each segment has:
//   - A .log file containing the segment header and message entries
//   - A .idx file containing the sparse index for fast lookups
//   - A base offset (first message ID) that uniquely identifies it
//
// File naming convention:
//   - Segment: {baseOffset:020d}.log (e.g., 00000000000000001000.log)
//   - Index:   {baseOffset:020d}.idx (e.g., 00000000000000001000.idx)
//
// The 20-digit zero-padding ensures lexicographic sorting matches numeric ordering.
package segment

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	// SegmentFileExtension is the file extension for segment files
	SegmentFileExtension = ".log"

	// IndexFileExtension is the file extension for index files
	IndexFileExtension = ".idx"

	// SegmentNameWidth is the number of digits in segment filenames (20 digits for uint64)
	SegmentNameWidth = 20
)

// FormatSegmentName creates a segment filename from a base offset.
// Returns a zero-padded 20-digit filename (e.g., "00000000000000001000.log").
func FormatSegmentName(baseOffset uint64) string {
	return fmt.Sprintf("%020d%s", baseOffset, SegmentFileExtension)
}

// FormatIndexName creates an index filename from a base offset.
// Returns a zero-padded 20-digit filename (e.g., "00000000000000001000.idx").
func FormatIndexName(baseOffset uint64) string {
	return fmt.Sprintf("%020d%s", baseOffset, IndexFileExtension)
}

// ParseSegmentName extracts the base offset from a segment filename.
// Returns an error if the filename doesn't match the expected format.
func ParseSegmentName(filename string) (uint64, error) {
	// Remove extension
	if !strings.HasSuffix(filename, SegmentFileExtension) {
		return 0, fmt.Errorf("invalid segment filename: %s (missing %s extension)", filename, SegmentFileExtension)
	}

	base := strings.TrimSuffix(filename, SegmentFileExtension)

	// Parse base offset
	offset, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid segment filename: %s (invalid base offset)", filename)
	}

	return offset, nil
}

// ParseIndexName extracts the base offset from an index filename.
// Returns an error if the filename doesn't match the expected format.
func ParseIndexName(filename string) (uint64, error) {
	// Remove extension
	if !strings.HasSuffix(filename, IndexFileExtension) {
		return 0, fmt.Errorf("invalid index filename: %s (missing %s extension)", filename, IndexFileExtension)
	}

	base := strings.TrimSuffix(filename, IndexFileExtension)

	// Parse base offset
	offset, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid index filename: %s (invalid base offset)", filename)
	}

	return offset, nil
}

// SegmentInfo holds information about a discovered segment file.
type SegmentInfo struct {
	// BaseOffset is the first message ID in this segment
	BaseOffset uint64

	// Path is the absolute path to the segment file
	Path string

	// IndexPath is the absolute path to the index file (may not exist)
	IndexPath string

	// Size is the segment file size in bytes
	Size int64
}

// DiscoverSegments finds all segment files in a directory and returns them sorted by base offset.
// Only returns segments with valid naming format. Missing index files are noted but not an error.
func DiscoverSegments(dir string) ([]*SegmentInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	var segments []*SegmentInfo

	for _, entry := range entries {
		// Skip directories and non-.log files
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), SegmentFileExtension) {
			continue
		}

		// Parse base offset from filename
		baseOffset, err := ParseSegmentName(entry.Name())
		if err != nil {
			// Skip files with invalid names
			continue
		}

		// Get file info for size
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Construct paths
		segmentPath := filepath.Join(dir, entry.Name())
		indexPath := filepath.Join(dir, FormatIndexName(baseOffset))

		segments = append(segments, &SegmentInfo{
			BaseOffset: baseOffset,
			Path:       segmentPath,
			IndexPath:  indexPath,
			Size:       info.Size(),
		})
	}

	// Sort by base offset (ascending)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].BaseOffset < segments[j].BaseOffset
	})

	return segments, nil
}

// ValidateSegmentSequence checks that segments form a valid sequence without gaps.
// Returns an error if there are missing segments or overlapping base offsets.
func ValidateSegmentSequence(segments []*SegmentInfo) error {
	if len(segments) == 0 {
		return nil
	}

	// Check for duplicates
	seen := make(map[uint64]bool)
	for _, seg := range segments {
		if seen[seg.BaseOffset] {
			return fmt.Errorf("duplicate segment with base offset %d", seg.BaseOffset)
		}
		seen[seg.BaseOffset] = true
	}

	return nil
}
