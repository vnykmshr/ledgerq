package queue

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	// MetadataFileName is the name of the metadata file
	MetadataFileName = "metadata.dat"

	// MetadataVersion is the current metadata format version
	MetadataVersion uint32 = 1

	// MetadataSize is the fixed size of metadata file
	MetadataSize = 24 // version(4) + nextMsgID(8) + readMsgID(8) + reserved(4)
)

// Metadata stores queue state for persistence
type Metadata struct {
	Version   uint32 // Format version
	NextMsgID uint64 // Next message ID to assign
	ReadMsgID uint64 // Next message ID to read
	mu        sync.RWMutex
	path      string
	file      *os.File
	autoSync  bool
}

// OpenMetadata opens or creates a metadata file
func OpenMetadata(dir string, autoSync bool) (*Metadata, error) {
	path := filepath.Join(dir, MetadataFileName)

	// Try to open existing file
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata file: %w", err)
	}

	m := &Metadata{
		path:     path,
		file:     file,
		autoSync: autoSync,
	}

	// Check if file is new (empty)
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat metadata file: %w", err)
	}

	if stat.Size() == 0 {
		// New file, initialize with defaults
		m.Version = MetadataVersion
		m.NextMsgID = 1
		m.ReadMsgID = 1

		if err := m.flush(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to initialize metadata: %w", err)
		}
	} else {
		// Existing file, read current state
		if err := m.load(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to load metadata: %w", err)
		}
	}

	return m, nil
}

// load reads metadata from disk
func (m *Metadata) load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Seek to beginning
	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}

	buf := make([]byte, MetadataSize)
	n, err := io.ReadFull(m.file, buf)
	if err != nil {
		return fmt.Errorf("read failed: %w", err)
	}
	if n != MetadataSize {
		return fmt.Errorf("incomplete read: got %d bytes, want %d", n, MetadataSize)
	}

	// Parse metadata
	m.Version = binary.LittleEndian.Uint32(buf[0:4])
	m.NextMsgID = binary.LittleEndian.Uint64(buf[4:12])
	m.ReadMsgID = binary.LittleEndian.Uint64(buf[12:20])
	// bytes 20-24 are reserved for future use

	// Validate version
	if m.Version != MetadataVersion {
		return fmt.Errorf("unsupported metadata version: %d (expected %d)", m.Version, MetadataVersion)
	}

	// Validate message IDs
	if m.NextMsgID == 0 || m.ReadMsgID == 0 {
		return fmt.Errorf("invalid message IDs: nextMsgID=%d, readMsgID=%d", m.NextMsgID, m.ReadMsgID)
	}

	return nil
}

// flush writes current metadata to disk
func (m *Metadata) flush() error {
	// Caller must hold lock

	buf := make([]byte, MetadataSize)

	binary.LittleEndian.PutUint32(buf[0:4], m.Version)
	binary.LittleEndian.PutUint64(buf[4:12], m.NextMsgID)
	binary.LittleEndian.PutUint64(buf[12:20], m.ReadMsgID)
	// bytes 20-24 reserved (zero-filled)

	// Seek to beginning
	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}

	// Write metadata
	n, err := m.file.Write(buf)
	if err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	if n != MetadataSize {
		return fmt.Errorf("incomplete write: wrote %d bytes, want %d", n, MetadataSize)
	}

	// Sync if auto-sync enabled
	if m.autoSync {
		if err := m.file.Sync(); err != nil {
			return fmt.Errorf("sync failed: %w", err)
		}
	}

	return nil
}

// GetState returns the current metadata state
func (m *Metadata) GetState() (nextMsgID, readMsgID uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.NextMsgID, m.ReadMsgID
}

// SetNextMsgID updates the next message ID and persists it
func (m *Metadata) SetNextMsgID(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.NextMsgID = id
	return m.flush()
}

// SetReadMsgID updates the read message ID and persists it
func (m *Metadata) SetReadMsgID(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ReadMsgID = id
	return m.flush()
}

// UpdateState updates both IDs and persists them atomically
func (m *Metadata) UpdateState(nextMsgID, readMsgID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.NextMsgID = nextMsgID
	m.ReadMsgID = readMsgID
	return m.flush()
}

// Sync forces a sync to disk
func (m *Metadata) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return fmt.Errorf("metadata file is closed")
	}

	return m.file.Sync()
}

// Close closes the metadata file
func (m *Metadata) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return nil
	}

	// Final sync before close
	if err := m.flush(); err != nil {
		_ = m.file.Close()
		m.file = nil
		return err
	}

	if err := m.file.Sync(); err != nil {
		_ = m.file.Close()
		m.file = nil
		return err
	}

	err := m.file.Close()
	m.file = nil
	return err
}
