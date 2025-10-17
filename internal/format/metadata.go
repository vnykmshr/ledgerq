package format

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// QueueMetadata represents the persistent state of a queue.
//
// This structure is serialized to JSON for debuggability and extensibility.
// Updates use double-buffering (write to .tmp, then atomic rename) for crash safety.
type QueueMetadata struct {
	// Version is the metadata format version
	Version uint16 `json:"version"`

	// WriteOffset is the next offset to write to (monotonically increasing)
	WriteOffset uint64 `json:"write_offset"`

	// ReadOffset is the next offset to read from
	ReadOffset uint64 `json:"read_offset"`

	// AckOffset is the highest contiguously acknowledged offset
	AckOffset uint64 `json:"ack_offset"`

	// ActiveSegmentID is the ID of the current write segment
	ActiveSegmentID uint64 `json:"active_segment_id"`

	// OldestSegmentID is the ID of the oldest segment (for cleanup)
	OldestSegmentID uint64 `json:"oldest_segment_id"`

	// MessageCount is the total number of messages written
	MessageCount uint64 `json:"message_count"`

	// AckCount is the total number of messages acknowledged
	AckCount uint64 `json:"ack_count"`

	// LastWriteTime is the Unix timestamp (nanoseconds) of the last write
	LastWriteTime int64 `json:"last_write_time"`

	// LastAckTime is the Unix timestamp (nanoseconds) of the last acknowledgment
	LastAckTime int64 `json:"last_ack_time"`

	// LastCompactTime is the Unix timestamp (nanoseconds) of the last compaction
	LastCompactTime int64 `json:"last_compact_time,omitempty"`
}

// NewQueueMetadata creates a new metadata structure with default values.
func NewQueueMetadata() *QueueMetadata {
	return &QueueMetadata{
		Version:         CurrentVersion,
		WriteOffset:     0,
		ReadOffset:      0,
		AckOffset:       0,
		ActiveSegmentID: 0,
		OldestSegmentID: 0,
		MessageCount:    0,
		AckCount:        0,
		LastWriteTime:   time.Now().UnixNano(),
		LastAckTime:     0,
		LastCompactTime: 0,
	}
}

// Validate checks if the metadata is consistent.
func (m *QueueMetadata) Validate() error {
	if m.Version == 0 {
		return fmt.Errorf("invalid version: %d", m.Version)
	}
	if m.Version > CurrentVersion {
		return fmt.Errorf("unsupported version: %d (current=%d)", m.Version, CurrentVersion)
	}
	if m.ReadOffset > m.WriteOffset {
		return fmt.Errorf("read offset (%d) > write offset (%d)", m.ReadOffset, m.WriteOffset)
	}
	if m.AckOffset > m.WriteOffset {
		return fmt.Errorf("ack offset (%d) > write offset (%d)", m.AckOffset, m.WriteOffset)
	}
	if m.AckCount > m.MessageCount {
		return fmt.Errorf("ack count (%d) > message count (%d)", m.AckCount, m.MessageCount)
	}
	if m.ActiveSegmentID < m.OldestSegmentID {
		return fmt.Errorf("active segment ID (%d) < oldest segment ID (%d)", m.ActiveSegmentID, m.OldestSegmentID)
	}
	return nil
}

// Marshal encodes the metadata to JSON with indentation for readability.
func (m *QueueMetadata) Marshal() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// UnmarshalMetadata decodes metadata from JSON.
func UnmarshalMetadata(data []byte) (*QueueMetadata, error) {
	meta := &QueueMetadata{}
	if err := json.Unmarshal(data, meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return meta, nil
}

// MetadataFile manages atomic reads and writes of queue metadata.
//
// Thread-safe implementation using mutex for concurrent access.
// Writes use double-buffering: write to .tmp file, fsync, then atomic rename.
type MetadataFile struct {
	path string
	mu   sync.RWMutex
}

// NewMetadataFile creates a new metadata file manager.
func NewMetadataFile(path string) *MetadataFile {
	return &MetadataFile{
		path: path,
	}
}

// Read reads and validates the metadata from disk.
//
// Returns an error if the file doesn't exist or contains invalid data.
func (mf *MetadataFile) Read() (*QueueMetadata, error) {
	mf.mu.RLock()
	defer mf.mu.RUnlock()

	return ReadMetadata(mf.path)
}

// Write atomically writes metadata to disk using double-buffering.
//
// Process:
//  1. Marshal metadata to JSON
//  2. Write to temporary file (.tmp)
//  3. Fsync temporary file
//  4. Atomic rename to final path
//  5. Fsync directory (ensures rename is durable)
func (mf *MetadataFile) Write(meta *QueueMetadata) error {
	mf.mu.Lock()
	defer mf.mu.Unlock()

	return WriteMetadata(mf.path, meta)
}

// ReadMetadata reads metadata from a file.
func ReadMetadata(path string) (*QueueMetadata, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: Path is user-provided for queue data
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	meta, err := UnmarshalMetadata(data)
	if err != nil {
		return nil, err
	}

	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	return meta, nil
}

// WriteMetadata atomically writes metadata to a file using double-buffering.
//
// This ensures crash safety: if the process crashes during write, the old
// metadata file remains intact, or the new one is complete.
func WriteMetadata(path string, meta *QueueMetadata) error {
	if err := meta.Validate(); err != nil {
		return fmt.Errorf("invalid metadata: %w", err)
	}

	// Marshal to JSON
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	// Create temporary file in the same directory
	tmpPath := path + ".tmp"

	// Write to temporary file
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) //nolint:gosec // G304: Path is user-provided for queue data
	if err != nil {
		return fmt.Errorf("failed to create temporary metadata file: %w", err)
	}

	// Write data
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Fsync to ensure data is on disk
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to sync metadata file: %w", err)
	}

	_ = f.Close()

	// Atomic rename (overwrites existing file)
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	// Fsync directory to ensure rename is durable
	dir := filepath.Dir(path)
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}

	return nil
}

// syncDir fsyncs a directory to ensure metadata operations are durable.
func syncDir(path string) error {
	d, err := os.Open(path) //nolint:gosec // G304: Path is user-provided for queue data
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()

	return d.Sync()
}

// ReadMetadataOrCreate reads metadata from a file, or creates a new one if it doesn't exist.
func ReadMetadataOrCreate(path string) (*QueueMetadata, error) {
	meta, err := ReadMetadata(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Create new metadata
			meta = NewQueueMetadata()
			if err := WriteMetadata(path, meta); err != nil {
				return nil, fmt.Errorf("failed to create metadata: %w", err)
			}
			return meta, nil
		}
		return nil, err
	}
	return meta, nil
}

// WriteTo writes metadata in a streaming fashion to the given writer (implements io.WriterTo).
// This is useful for testing and debugging.
func (m *QueueMetadata) WriteTo(w io.Writer) (int64, error) {
	data, err := m.Marshal()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(data)
	return int64(n), err
}

// ReadFrom reads metadata from a reader.
func ReadFrom(r io.Reader) (*QueueMetadata, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	return UnmarshalMetadata(data)
}
