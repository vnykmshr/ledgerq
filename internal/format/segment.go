package format

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Magic numbers for file type identification
const (
	SegmentMagic  uint32 = 0x4C445151 // "LDQQ" - LedgerQ Queue
	IndexMagic    uint32 = 0x4C445149 // "LDQI" - LedgerQ Index
	MetadataMagic uint32 = 0x4C44514D // "LDQM" - LedgerQ Metadata
)

// Format version
const (
	FormatVersion1 uint16 = 1
	CurrentVersion uint16 = FormatVersion1
)

// Segment header flags
const (
	SegmentFlagNone       uint16 = 0
	SegmentFlagCompressed uint16 = 1 << 0
	SegmentFlagEncrypted  uint16 = 1 << 1
)

// SegmentHeaderSize is the fixed size of the segment header (64 bytes - cache-line aligned)
// Layout: Magic(4) + Version(2) + Flags(2) + BaseOffset(8) + CreatedAt(8) +
//
//	MinMsgID(8) + MaxMsgID(8) + Reserved(20) + HeaderCRC(4) = 64 bytes
const SegmentHeaderSize = 64

// SegmentHeader represents the header of a segment file.
//
// Binary format (little-endian, 64 bytes):
//
//	[Magic:4][Version:2][Flags:2][BaseOffset:8][CreatedAt:8]
//	[MinMsgID:8][MaxMsgID:8][Reserved:20][HeaderCRC:4]
type SegmentHeader struct {
	// Magic is the file format identifier (0x4C445151 for segments)
	Magic uint32

	// Version is the format version
	Version uint16

	// Flags contains segment properties (compressed, encrypted, etc.)
	Flags uint16

	// BaseOffset is the starting offset of this segment in the queue
	BaseOffset uint64

	// CreatedAt is the Unix timestamp (nanoseconds) when segment was created
	CreatedAt int64

	// MinMsgID is the smallest message ID in this segment
	MinMsgID uint64

	// MaxMsgID is the largest message ID in this segment
	MaxMsgID uint64

	// Reserved is space for future extensions (20 bytes)
	Reserved [20]byte

	// HeaderCRC is the checksum of the header (excluding this field)
	HeaderCRC uint32
}

// NewSegmentHeader creates a new segment header with default values.
func NewSegmentHeader(baseOffset uint64) *SegmentHeader {
	return &SegmentHeader{
		Magic:      SegmentMagic,
		Version:    CurrentVersion,
		Flags:      SegmentFlagNone,
		BaseOffset: baseOffset,
		MinMsgID:   0,
		MaxMsgID:   0,
	}
}

// Marshal encodes the segment header into binary format with CRC32C checksum.
func (h *SegmentHeader) Marshal() []byte {
	buf := make([]byte, SegmentHeaderSize)
	offset := 0

	// Write all fields
	binary.LittleEndian.PutUint32(buf[offset:], h.Magic)
	offset += 4
	binary.LittleEndian.PutUint16(buf[offset:], h.Version)
	offset += 2
	binary.LittleEndian.PutUint16(buf[offset:], h.Flags)
	offset += 2
	binary.LittleEndian.PutUint64(buf[offset:], h.BaseOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(h.CreatedAt)) //nolint:gosec // G115: Safe uint64 conversion
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], h.MinMsgID)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], h.MaxMsgID)
	offset += 8
	copy(buf[offset:], h.Reserved[:])
	offset += 20

	// Compute and write CRC32C over everything except the CRC itself
	crc := ComputeCRC32C(buf[:offset])
	binary.LittleEndian.PutUint32(buf[offset:], crc)
	h.HeaderCRC = crc

	return buf
}

// UnmarshalSegmentHeader decodes a segment header from the given reader.
func UnmarshalSegmentHeader(r io.Reader) (*SegmentHeader, error) {
	buf := make([]byte, SegmentHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read segment header: %w", err)
	}

	// Verify CRC first
	storedCRC := binary.LittleEndian.Uint32(buf[SegmentHeaderSize-4:])
	computedCRC := ComputeCRC32C(buf[:SegmentHeaderSize-4])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("segment header CRC mismatch: stored=%08x computed=%08x", storedCRC, computedCRC)
	}

	// Parse header fields
	offset := 0
	header := &SegmentHeader{}
	header.Magic = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	header.Version = binary.LittleEndian.Uint16(buf[offset:])
	offset += 2
	header.Flags = binary.LittleEndian.Uint16(buf[offset:])
	offset += 2
	header.BaseOffset = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	header.CreatedAt = int64(binary.LittleEndian.Uint64(buf[offset:])) //nolint:gosec // G115: Safe int64 conversion
	offset += 8
	header.MinMsgID = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	header.MaxMsgID = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	copy(header.Reserved[:], buf[offset:offset+20])
	_ = offset // Last use, suppress ineffassign warning
	header.HeaderCRC = storedCRC

	return header, nil
}

// ReadSegmentHeader reads and validates a segment header from a file.
func ReadSegmentHeader(path string) (*SegmentHeader, error) {
	f, err := os.Open(path) //nolint:gosec // G304: Path is user-provided for queue data
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	return UnmarshalSegmentHeader(f)
}

// WriteSegmentHeader writes a segment header to a file.
func WriteSegmentHeader(path string, header *SegmentHeader) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) //nolint:gosec // G304: Path is user-provided for queue data
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	data := header.Marshal()
	if _, err := f.Write(data); err != nil {
		return err
	}

	return f.Sync()
}

// Validate checks if the segment header is valid.
func (h *SegmentHeader) Validate() error {
	if h.Magic != SegmentMagic {
		return fmt.Errorf("invalid magic number: got=%08x want=%08x", h.Magic, SegmentMagic)
	}
	if h.Version == 0 {
		return fmt.Errorf("invalid version: %d", h.Version)
	}
	if h.Version > CurrentVersion {
		return fmt.Errorf("unsupported version: %d (current=%d)", h.Version, CurrentVersion)
	}
	if h.MinMsgID > h.MaxMsgID && h.MaxMsgID != 0 {
		return fmt.Errorf("invalid message ID range: min=%d max=%d", h.MinMsgID, h.MaxMsgID)
	}
	return nil
}
