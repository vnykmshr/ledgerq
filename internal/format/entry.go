package format

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Entry types
const (
	EntryTypeData       uint8 = 1 // Regular data message
	EntryTypeTombstone  uint8 = 2 // Deletion marker
	EntryTypeCheckpoint uint8 = 3 // Checkpoint marker
)

// Entry flags
const (
	EntryFlagNone       uint8 = 0
	EntryFlagCompressed uint8 = 1 << 0 // Payload is compressed
	EntryFlagEncrypted  uint8 = 1 << 1 // Payload is encrypted
)

// EntryHeaderSize is the size of the entry header in bytes (22 bytes).
// Layout: Type(1) + Flags(1) + MsgID(8) + Timestamp(8) = 22 bytes
const EntryHeaderSize = 22

// Entry represents a single message entry in a segment file.
//
// Binary format (little-endian):
//   [Length:4][Type:1][Flags:1][MsgID:8][Timestamp:8][Payload:N][CRC32C:4]
//
// Total header size: 22 bytes
// Total entry size: 22 + len(Payload) + 4 bytes
type Entry struct {
	// Length is the total size of the entry including header and CRC (excludes the length field itself)
	Length uint32

	// Type indicates the entry type (Data, Tombstone, Checkpoint)
	Type uint8

	// Flags contains entry flags (compressed, encrypted, etc.)
	Flags uint8

	// MsgID is the unique message identifier
	MsgID uint64

	// Timestamp is the Unix time in nanoseconds when the entry was created
	Timestamp int64

	// Payload is the message data
	Payload []byte
}

// Marshal encodes the entry into binary format with CRC32C checksum.
// Returns the complete entry bytes ready to be written to disk.
func (e *Entry) Marshal() []byte {
	totalLen := 4 + EntryHeaderSize + len(e.Payload) + 4 // length field + header + payload + crc
	buf := make([]byte, totalLen)

	// Calculate and set the Length field (excludes the length field itself)
	e.Length = uint32(EntryHeaderSize + len(e.Payload) + 4) //nolint:gosec // G115: Safe conversion, payload limited by file size

	// Write all fields (length is at offset 0, header starts at offset 4)
	offset := 0
	binary.LittleEndian.PutUint32(buf[offset:], e.Length)
	offset += 4

	buf[offset] = e.Type
	offset++
	buf[offset] = e.Flags
	offset++
	binary.LittleEndian.PutUint64(buf[offset:], e.MsgID)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(e.Timestamp)) //nolint:gosec // G115: Safe uint64 conversion
	offset += 8

	// Write payload
	copy(buf[offset:], e.Payload)
	offset += len(e.Payload)

	// Compute and write CRC32C over everything except the CRC itself
	crc := ComputeCRC32C(buf[:offset])
	binary.LittleEndian.PutUint32(buf[offset:], crc)

	return buf
}

// Unmarshal decodes an entry from the given reader.
// Returns an error if the entry is corrupted or invalid.
func Unmarshal(r io.Reader) (*Entry, error) {
	// Read length field
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read entry length: %w", err)
	}

	// Validate length (must be at least header + crc)
	if length < EntryHeaderSize+4 {
		return nil, fmt.Errorf("invalid entry length: %d (minimum %d)", length, EntryHeaderSize+4)
	}

	// Read rest of entry (header + payload + crc)
	buf := make([]byte, length)
	binary.LittleEndian.PutUint32(buf[0:4], length) // Restore length field for CRC
	if _, err := io.ReadFull(r, buf[4:]); err != nil {
		return nil, fmt.Errorf("failed to read entry data: %w", err)
	}

	// Verify CRC
	storedCRC := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	computedCRC := ComputeCRC32C(buf[:len(buf)-4])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("entry CRC mismatch: stored=%08x computed=%08x", storedCRC, computedCRC)
	}

	// Parse entry fields
	entry := &Entry{
		Length:    length,
		Type:      buf[4],
		Flags:     buf[5],
		MsgID:     binary.LittleEndian.Uint64(buf[6:14]),
		Timestamp: int64(binary.LittleEndian.Uint64(buf[14:22])), //nolint:gosec // G115: Safe int64 conversion
	}

	// Extract payload (everything between header and CRC)
	payloadLen := len(buf) - EntryHeaderSize - 4
	if payloadLen > 0 {
		entry.Payload = make([]byte, payloadLen)
		copy(entry.Payload, buf[22:len(buf)-4])
	}

	return entry, nil
}

// TotalSize returns the total size of the entry on disk (including length field).
func (e *Entry) TotalSize() int {
	return 4 + int(e.Length) // length field + entry content
}

// Validate checks if the entry is valid.
func (e *Entry) Validate() error {
	if e.Type != EntryTypeData && e.Type != EntryTypeTombstone && e.Type != EntryTypeCheckpoint {
		return fmt.Errorf("invalid entry type: %d", e.Type)
	}
	if e.MsgID == 0 {
		return fmt.Errorf("message ID cannot be zero")
	}
	if e.Timestamp <= 0 {
		return fmt.Errorf("invalid timestamp: %d", e.Timestamp)
	}
	expectedLength := uint32(EntryHeaderSize + len(e.Payload) + 4) //nolint:gosec // G115: Safe conversion, payload limited by file size
	if e.Length != expectedLength {
		return fmt.Errorf("length mismatch: got %d, expected %d", e.Length, expectedLength)
	}
	return nil
}
