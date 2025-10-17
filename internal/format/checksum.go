// Package format provides binary encoding/decoding for LedgerQ file formats.
//
// This package implements:
//   - Entry format: message entries with CRC32C checksums
//   - Segment format: append-only log segments with headers
//   - Index format: sparse indexes for offset-to-position mapping
//   - Metadata format: JSON-encoded queue state with atomic updates
//   - Checksum utilities: CRC32C (Castagnoli) computation and verification
package format

import "hash/crc32"

// CRC32C table using Castagnoli polynomial.
// This is hardware-accelerated on modern Intel (SSE 4.2) and ARM processors.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// ComputeCRC32C computes a CRC32C checksum over the given data.
// Uses the Castagnoli polynomial for better performance on modern hardware.
func ComputeCRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// VerifyCRC32C verifies that the computed CRC matches the expected value.
func VerifyCRC32C(data []byte, expected uint32) bool {
	return ComputeCRC32C(data) == expected
}
