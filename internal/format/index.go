package format

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// IndexHeaderSize is the fixed size of the index file header (32 bytes)
const IndexHeaderSize = 32

// IndexEntrySize is the fixed size of each index entry (24 bytes)
const IndexEntrySize = 24

// DefaultIndexInterval is the default number of bytes between index entries (4KB)
const DefaultIndexInterval = 4096

// IndexHeader represents the header of an index file.
//
// Binary format (little-endian, 32 bytes):
//   [Magic:4][Version:2][_:2][BaseID:8][EntryCount:8][Reserved:4][HeaderCRC:4]
type IndexHeader struct {
	// Magic is the file format identifier (0x4C445149 for indexes)
	Magic uint32

	// Version is the format version
	Version uint16

	// Padding for alignment
	_ uint16

	// BaseID is the base message ID for this index
	BaseID uint64

	// EntryCount is the number of index entries
	EntryCount uint64

	// Reserved is space for future extensions (4 bytes)
	Reserved uint32

	// HeaderCRC is the checksum of the header (excluding this field)
	HeaderCRC uint32
}

// IndexEntry represents a single entry in the sparse index.
//
// Binary format (little-endian, 24 bytes):
//   [MessageID:8][FileOffset:8][Timestamp:8]
type IndexEntry struct {
	// MessageID is the message identifier
	MessageID uint64

	// FileOffset is the byte position in the segment file
	FileOffset uint64

	// Timestamp is the Unix time in nanoseconds
	Timestamp int64
}

// NewIndexHeader creates a new index header with default values.
func NewIndexHeader(baseID uint64) *IndexHeader {
	return &IndexHeader{
		Magic:      IndexMagic,
		Version:    CurrentVersion,
		BaseID:     baseID,
		EntryCount: 0,
	}
}

// Marshal encodes the index header into binary format with CRC32C checksum.
func (h *IndexHeader) Marshal() []byte {
	buf := make([]byte, IndexHeaderSize)
	offset := 0

	// Write all fields
	binary.LittleEndian.PutUint32(buf[offset:], h.Magic)
	offset += 4
	binary.LittleEndian.PutUint16(buf[offset:], h.Version)
	offset += 2
	offset += 2 // Skip padding
	binary.LittleEndian.PutUint64(buf[offset:], h.BaseID)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], h.EntryCount)
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], h.Reserved)
	offset += 4

	// Compute and write CRC32C
	crc := ComputeCRC32C(buf[:offset])
	binary.LittleEndian.PutUint32(buf[offset:], crc)
	h.HeaderCRC = crc

	return buf
}

// UnmarshalIndexHeader decodes an index header from the given reader.
func UnmarshalIndexHeader(r io.Reader) (*IndexHeader, error) {
	buf := make([]byte, IndexHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read index header: %w", err)
	}

	// Verify CRC
	storedCRC := binary.LittleEndian.Uint32(buf[IndexHeaderSize-4:])
	computedCRC := ComputeCRC32C(buf[:IndexHeaderSize-4])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("index header CRC mismatch: stored=%08x computed=%08x", storedCRC, computedCRC)
	}

	// Parse header fields
	offset := 0
	header := &IndexHeader{}
	header.Magic = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	header.Version = binary.LittleEndian.Uint16(buf[offset:])
	offset += 2
	offset += 2 // Skip padding
	header.BaseID = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	header.EntryCount = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	header.Reserved = binary.LittleEndian.Uint32(buf[offset:])
	// offset += 4 // Last field before CRC, not needed
	header.HeaderCRC = storedCRC

	return header, nil
}

// Validate checks if the index header is valid.
func (h *IndexHeader) Validate() error {
	if h.Magic != IndexMagic {
		return fmt.Errorf("invalid magic number: got=%08x want=%08x", h.Magic, IndexMagic)
	}
	if h.Version == 0 {
		return fmt.Errorf("invalid version: %d", h.Version)
	}
	if h.Version > CurrentVersion {
		return fmt.Errorf("unsupported version: %d (current=%d)", h.Version, CurrentVersion)
	}
	return nil
}

// Marshal encodes an index entry into binary format.
func (e *IndexEntry) Marshal() []byte {
	buf := make([]byte, IndexEntrySize)
	offset := 0

	binary.LittleEndian.PutUint64(buf[offset:], e.MessageID)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], e.FileOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(e.Timestamp)) //nolint:gosec // G115: Safe uint64 conversion

	return buf
}

// UnmarshalIndexEntry decodes an index entry from the given reader.
func UnmarshalIndexEntry(r io.Reader) (*IndexEntry, error) {
	buf := make([]byte, IndexEntrySize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read index entry: %w", err)
	}

	offset := 0
	entry := &IndexEntry{}
	entry.MessageID = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	entry.FileOffset = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	entry.Timestamp = int64(binary.LittleEndian.Uint64(buf[offset:])) //nolint:gosec // G115: Safe int64 conversion

	return entry, nil
}

// Index represents an in-memory sparse index.
type Index struct {
	Header  *IndexHeader
	Entries []*IndexEntry
}

// NewIndex creates a new index with the given base ID.
func NewIndex(baseID uint64) *Index {
	return &Index{
		Header:  NewIndexHeader(baseID),
		Entries: make([]*IndexEntry, 0),
	}
}

// Add adds an entry to the index.
func (idx *Index) Add(entry *IndexEntry) {
	idx.Entries = append(idx.Entries, entry)
	idx.Header.EntryCount = uint64(len(idx.Entries))
}

// Find performs a binary search to find the entry for the given message ID.
// Returns the entry with the largest ID that is <= the given ID.
// Returns nil if no such entry exists.
func (idx *Index) Find(messageID uint64) *IndexEntry {
	if len(idx.Entries) == 0 {
		return nil
	}

	// Binary search for the largest entry with ID <= messageID
	left, right := 0, len(idx.Entries)-1
	var result *IndexEntry

	for left <= right {
		mid := left + (right-left)/2
		entry := idx.Entries[mid]

		if entry.MessageID == messageID {
			return entry
		} else if entry.MessageID < messageID {
			result = entry
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return result
}

// WriteTo writes the index to the given writer (implements io.WriterTo).
func (idx *Index) WriteTo(w io.Writer) (int64, error) {
	var written int64

	// Write header
	headerData := idx.Header.Marshal()
	n, err := w.Write(headerData)
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("failed to write index header: %w", err)
	}

	// Write entries
	for _, entry := range idx.Entries {
		entryData := entry.Marshal()
		n, err := w.Write(entryData)
		written += int64(n)
		if err != nil {
			return written, fmt.Errorf("failed to write index entry: %w", err)
		}
	}

	return written, nil
}

// ReadIndex reads an index from the given reader.
func ReadIndex(r io.Reader) (*Index, error) {
	// Read header
	header, err := UnmarshalIndexHeader(r)
	if err != nil {
		return nil, err
	}

	if err := header.Validate(); err != nil {
		return nil, err
	}

	// Read entries
	entries := make([]*IndexEntry, header.EntryCount)
	for i := uint64(0); i < header.EntryCount; i++ {
		entry, err := UnmarshalIndexEntry(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read entry %d: %w", i, err)
		}
		entries[i] = entry
	}

	return &Index{
		Header:  header,
		Entries: entries,
	}, nil
}

// ReadIndexFile reads an index from a file.
func ReadIndexFile(path string) (*Index, error) {
	f, err := os.Open(path) //nolint:gosec // G304: Path is user-provided for queue data
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	return ReadIndex(f)
}

// WriteIndexFile writes an index to a file.
func WriteIndexFile(path string, idx *Index) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) //nolint:gosec // G304: Path is user-provided for queue data
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := idx.WriteTo(f); err != nil {
		return err
	}

	return f.Sync()
}
