package format

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"
)

func TestCompressionType_String(t *testing.T) {
	tests := []struct {
		name string
		ct   CompressionType
		want string
	}{
		{"none", CompressionNone, "none"},
		{"gzip", CompressionGzip, "gzip"},
		{"unknown", CompressionType(99), "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ct.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressPayload_None(t *testing.T) {
	payload := []byte("test data")
	compressed, err := CompressPayload(payload, CompressionNone, 0)
	if err != nil {
		t.Fatalf("CompressPayload failed: %v", err)
	}

	if !bytes.Equal(compressed, payload) {
		t.Errorf("CompressionNone should return original payload")
	}
}

func TestCompressPayload_Gzip(t *testing.T) {
	payload := []byte(strings.Repeat("test data ", 100))

	tests := []struct {
		name  string
		level int
	}{
		{"default level", 0},
		{"level 1 (fastest)", gzip.BestSpeed},
		{"level 6 (default)", gzip.DefaultCompression},
		{"level 9 (best)", gzip.BestCompression},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := CompressPayload(payload, CompressionGzip, tt.level)
			if err != nil {
				t.Fatalf("CompressPayload failed: %v", err)
			}

			// Compressed should be smaller than original for repetitive data
			if len(compressed) >= len(payload) {
				t.Errorf("Compressed size (%d) should be smaller than original (%d)",
					len(compressed), len(payload))
			}

			// Verify it's valid gzip data
			decompressed, err := DecompressPayload(compressed, CompressionGzip)
			if err != nil {
				t.Fatalf("DecompressPayload failed: %v", err)
			}

			if !bytes.Equal(decompressed, payload) {
				t.Errorf("Roundtrip failed: decompressed != original")
			}
		})
	}
}

func TestCompressPayload_InvalidLevel(t *testing.T) {
	payload := []byte("test")

	tests := []struct {
		name  string
		level int
	}{
		{"level too low", -3},
		{"level too high", 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompressPayload(payload, CompressionGzip, tt.level)
			if err == nil {
				t.Error("Expected error for invalid compression level")
			}
		})
	}
}

func TestCompressPayload_UnsupportedType(t *testing.T) {
	payload := []byte("test")
	_, err := CompressPayload(payload, CompressionType(99), 0)
	if err == nil {
		t.Error("Expected error for unsupported compression type")
	}
	if !strings.Contains(err.Error(), "unsupported compression type") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestDecompressPayload_None(t *testing.T) {
	payload := []byte("test data")
	decompressed, err := DecompressPayload(payload, CompressionNone)
	if err != nil {
		t.Fatalf("DecompressPayload failed: %v", err)
	}

	if !bytes.Equal(decompressed, payload) {
		t.Errorf("CompressionNone should return original payload")
	}
}

func TestDecompressPayload_Gzip(t *testing.T) {
	original := []byte(strings.Repeat("test data ", 100))

	// Compress first
	compressed, err := CompressPayload(original, CompressionGzip, gzip.DefaultCompression)
	if err != nil {
		t.Fatalf("CompressPayload failed: %v", err)
	}

	// Decompress
	decompressed, err := DecompressPayload(compressed, CompressionGzip)
	if err != nil {
		t.Fatalf("DecompressPayload failed: %v", err)
	}

	if !bytes.Equal(decompressed, original) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

func TestDecompressPayload_CorruptedData(t *testing.T) {
	// Invalid gzip data
	corrupted := []byte("not valid gzip data")
	_, err := DecompressPayload(corrupted, CompressionGzip)
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

func TestDecompressPayload_UnsupportedType(t *testing.T) {
	payload := []byte("test")
	_, err := DecompressPayload(payload, CompressionType(99))
	if err == nil {
		t.Error("Expected error for unsupported compression type")
	}
}

func TestDecompressPayload_DecompressionBomb(t *testing.T) {
	// Create a payload that would decompress to more than MaxDecompressedSize
	// We'll use a highly repetitive pattern that compresses well
	largePayload := bytes.Repeat([]byte("A"), MaxDecompressedSize+1024*1024)

	compressed, err := CompressPayload(largePayload, CompressionGzip, gzip.BestSpeed)
	if err != nil {
		t.Fatalf("CompressPayload failed: %v", err)
	}

	// Try to decompress - should fail due to size limit
	_, err = DecompressPayload(compressed, CompressionGzip)
	if err == nil {
		t.Error("Expected error for decompression bomb")
	}
	if !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Errorf("Expected decompression bomb error, got: %v", err)
	}
}

func TestShouldCompress(t *testing.T) {
	tests := []struct {
		name           string
		originalSize   int
		compressedSize int
		minSize        int
		want           bool
	}{
		{
			name:           "below minimum size",
			originalSize:   512,
			compressedSize: 256,
			minSize:        1024,
			want:           false,
		},
		{
			name:           "at minimum size, good compression",
			originalSize:   1024,
			compressedSize: 500,
			minSize:        1024,
			want:           true,
		},
		{
			name:           "above minimum, good compression",
			originalSize:   10000,
			compressedSize: 5000,
			minSize:        1024,
			want:           true,
		},
		{
			name:           "above minimum, marginal compression (4%)",
			originalSize:   10000,
			compressedSize: 9600,
			minSize:        1024,
			want:           false, // Less than 5% savings
		},
		{
			name:           "above minimum, exactly 5% compression",
			originalSize:   10000,
			compressedSize: 9500,
			minSize:        1024,
			want:           false, // Threshold is < 95%, not <=
		},
		{
			name:           "above minimum, just over 5% compression",
			originalSize:   10000,
			compressedSize: 9499,
			minSize:        1024,
			want:           true,
		},
		{
			name:           "compressed larger than original",
			originalSize:   1024,
			compressedSize: 1100,
			minSize:        1024,
			want:           false,
		},
		{
			name:           "zero minimum size, good compression",
			originalSize:   100,
			compressedSize: 50,
			minSize:        0,
			want:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldCompress(tt.originalSize, tt.compressedSize, tt.minSize)
			if got != tt.want {
				t.Errorf("ShouldCompress(%d, %d, %d) = %v, want %v",
					tt.originalSize, tt.compressedSize, tt.minSize, got, tt.want)
			}
		})
	}
}

func TestCompression_Roundtrip(t *testing.T) {
	testCases := []struct {
		name    string
		payload []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello")},
		{"medium", []byte(strings.Repeat("test data ", 100))},
		{"large", []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 1000))},
		{"binary", []byte{0, 1, 2, 3, 255, 254, 253}},
		{"json-like", []byte(`{"key":"value","array":[1,2,3],"nested":{"field":"data"}}`)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Compress
			compressed, err := CompressPayload(tc.payload, CompressionGzip, gzip.DefaultCompression)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := DecompressPayload(compressed, CompressionGzip)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			// Verify
			if !bytes.Equal(decompressed, tc.payload) {
				t.Errorf("Roundtrip failed: got %v, want %v", decompressed, tc.payload)
			}
		})
	}
}

func TestCompression_CompressionRatios(t *testing.T) {
	testCases := []struct {
		name                string
		payload             []byte
		minCompressionRatio float64 // Minimum expected compression (compressed/original)
	}{
		{
			name:                "highly repetitive",
			payload:             bytes.Repeat([]byte("A"), 10000),
			minCompressionRatio: 0.01, // Should compress to ~1% or less
		},
		{
			name:                "json data",
			payload:             []byte(strings.Repeat(`{"key":"value","number":12345}`, 100)),
			minCompressionRatio: 0.3, // Should compress to ~30% or less
		},
		{
			name:                "text data",
			payload:             []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 100)),
			minCompressionRatio: 0.5, // Should compress to ~50% or less
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := CompressPayload(tc.payload, CompressionGzip, gzip.DefaultCompression)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			ratio := float64(len(compressed)) / float64(len(tc.payload))
			if ratio > tc.minCompressionRatio {
				t.Errorf("Compression ratio %.2f is worse than expected %.2f for %s",
					ratio, tc.minCompressionRatio, tc.name)
			}
			t.Logf("%s: Original=%d, Compressed=%d, Ratio=%.2f%%",
				tc.name, len(tc.payload), len(compressed), ratio*100)
		})
	}
}

// Benchmark compression performance
func BenchmarkCompressPayload_Gzip_1KB(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 100) // ~1KB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressPayload(payload, CompressionGzip, gzip.DefaultCompression)
	}
}

func BenchmarkCompressPayload_Gzip_10KB(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 1000) // ~10KB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressPayload(payload, CompressionGzip, gzip.DefaultCompression)
	}
}

func BenchmarkCompressPayload_Gzip_100KB(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 10000) // ~100KB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressPayload(payload, CompressionGzip, gzip.DefaultCompression)
	}
}

func BenchmarkDecompressPayload_Gzip_1KB(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 100)
	compressed, _ := CompressPayload(payload, CompressionGzip, gzip.DefaultCompression)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressPayload(compressed, CompressionGzip)
	}
}

func BenchmarkDecompressPayload_Gzip_10KB(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 1000)
	compressed, _ := CompressPayload(payload, CompressionGzip, gzip.DefaultCompression)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressPayload(compressed, CompressionGzip)
	}
}

func BenchmarkDecompressPayload_Gzip_100KB(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 10000)
	compressed, _ := CompressPayload(payload, CompressionGzip, gzip.DefaultCompression)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressPayload(compressed, CompressionGzip)
	}
}

// Benchmark different compression levels
func BenchmarkCompressPayload_Level1(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressPayload(payload, CompressionGzip, gzip.BestSpeed)
	}
}

func BenchmarkCompressPayload_Level6(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressPayload(payload, CompressionGzip, gzip.DefaultCompression)
	}
}

func BenchmarkCompressPayload_Level9(b *testing.B) {
	payload := bytes.Repeat([]byte("test data "), 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressPayload(payload, CompressionGzip, gzip.BestCompression)
	}
}
