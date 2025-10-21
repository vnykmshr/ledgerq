package format

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// CompressionType represents the compression algorithm used for a message payload.
type CompressionType uint8

const (
	// CompressionNone indicates no compression (default)
	CompressionNone CompressionType = 0

	// CompressionGzip indicates GZIP compression (compress/gzip)
	CompressionGzip CompressionType = 1

	// CompressionSnappy is reserved for future use (would require external dependency)
	// CompressionSnappy CompressionType = 2

	// CompressionLZ4 is reserved for future use (would require external dependency)
	// CompressionLZ4 CompressionType = 3
)

// MaxDecompressedSize is the maximum size allowed for a decompressed payload
// to prevent decompression bomb attacks.
const MaxDecompressedSize = 100 * 1024 * 1024 // 100 MB

// String returns the string representation of the compression type.
func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGzip:
		return "gzip"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

// CompressPayload compresses a payload using the specified algorithm and level.
// Returns the compressed data or an error if compression fails.
//
// Parameters:
//   - payload: The data to compress
//   - compression: The compression algorithm to use
//   - level: The compression level (algorithm-specific, 0 = default)
//
// For GZIP:
//   - Level 1 (gzip.BestSpeed): Fastest, ~50% compression
//   - Level 6 (gzip.DefaultCompression): Balanced, ~60-70% compression
//   - Level 9 (gzip.BestCompression): Best ratio, ~65-75% compression
func CompressPayload(payload []byte, compression CompressionType, level int) ([]byte, error) {
	if compression == CompressionNone {
		return payload, nil
	}

	switch compression {
	case CompressionGzip:
		return compressGzip(payload, level)
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compression)
	}
}

// compressGzip compresses data using GZIP compression.
func compressGzip(payload []byte, level int) ([]byte, error) {
	var buf bytes.Buffer

	// Use default level if not specified
	if level == 0 {
		level = gzip.DefaultCompression
	}

	// Validate level
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		return nil, fmt.Errorf("invalid gzip compression level: %d (valid range: %d-%d)",
			level, gzip.HuffmanOnly, gzip.BestCompression)
	}

	writer, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip writer: %w", err)
	}

	if _, err := writer.Write(payload); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to compress payload: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	return buf.Bytes(), nil
}

// DecompressPayload decompresses a payload using the specified algorithm.
// Returns the decompressed data or an error if decompression fails.
//
// This function includes protection against decompression bombs by limiting
// the maximum decompressed size to MaxDecompressedSize.
func DecompressPayload(payload []byte, compression CompressionType) ([]byte, error) {
	if compression == CompressionNone {
		return payload, nil
	}

	switch compression {
	case CompressionGzip:
		return decompressGzip(payload)
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compression)
	}
}

// decompressGzip decompresses data using GZIP decompression.
func decompressGzip(payload []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer reader.Close()

	// Use LimitReader to prevent decompression bombs
	limitedReader := io.LimitReader(reader, MaxDecompressedSize+1)

	var buf bytes.Buffer
	n, err := io.Copy(&buf, limitedReader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress payload: %w", err)
	}

	// Check if we hit the size limit
	if n > MaxDecompressedSize {
		return nil, fmt.Errorf("decompressed payload exceeds maximum size of %d bytes", MaxDecompressedSize)
	}

	return buf.Bytes(), nil
}

// ShouldCompress determines whether a payload should be compressed based on
// the minimum size threshold and compression efficiency.
//
// Parameters:
//   - originalSize: Size of the original payload in bytes
//   - compressedSize: Size of the compressed payload in bytes
//   - minSize: Minimum payload size to consider for compression
//
// Returns true if:
//  1. Original size >= minSize
//  2. Compressed size < original size * 0.95 (at least 5% savings)
func ShouldCompress(originalSize, compressedSize, minSize int) bool {
	// Don't compress if below minimum size threshold
	if originalSize < minSize {
		return false
	}

	// Don't compress if savings are less than 5%
	// This avoids the CPU overhead when compression doesn't help
	threshold := float64(originalSize) * 0.95
	return float64(compressedSize) < threshold
}
