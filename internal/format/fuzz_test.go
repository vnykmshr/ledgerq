package format

import (
	"bytes"
	"testing"
)

// FuzzEntry tests the Entry encoding/decoding with fuzz inputs
func FuzzEntry(f *testing.F) {
	// Add seed corpus with valid entries
	f.Add([]byte("hello world"), uint64(1), int64(1234567890))
	f.Add([]byte(""), uint64(0), int64(0))
	f.Add([]byte("a"), uint64(999), int64(-1))
	f.Add(make([]byte, 1024), uint64(12345), int64(1000000))

	f.Fuzz(func(t *testing.T, payload []byte, msgID uint64, timestamp int64) {
		// Skip if payload is too large (avoid OOM)
		if len(payload) > 10*1024*1024 {
			t.Skip()
		}

		// Create entry
		entry := &Entry{
			Type:      EntryTypeData,
			Flags:     EntryFlagNone,
			MsgID:     msgID,
			Timestamp: timestamp,
			Payload:   payload,
		}

		// Encode
		encoded := entry.Marshal()

		// Decode
		decoded, err := Unmarshal(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("DecodeEntry failed: %v (encoded len: %d)", err, len(encoded))
		}

		// Verify round-trip
		if decoded.Type != entry.Type {
			t.Errorf("Type mismatch: got %d, want %d", decoded.Type, entry.Type)
		}
		if decoded.Flags != entry.Flags {
			t.Errorf("Flags mismatch: got %d, want %d", decoded.Flags, entry.Flags)
		}
		if decoded.MsgID != entry.MsgID {
			t.Errorf("MsgID mismatch: got %d, want %d", decoded.MsgID, entry.MsgID)
		}
		if decoded.Timestamp != entry.Timestamp {
			t.Errorf("Timestamp mismatch: got %d, want %d", decoded.Timestamp, entry.Timestamp)
		}
		if !bytes.Equal(decoded.Payload, entry.Payload) {
			t.Errorf("Payload mismatch: got %d bytes, want %d bytes", len(decoded.Payload), len(entry.Payload))
		}
	})
}

// FuzzUnmarshal tests decoding with random/malformed input
func FuzzUnmarshal(f *testing.F) {
	// Add some seed corpus
	validEntry := &Entry{
		Type:      EntryTypeData,
		Flags:     EntryFlagNone,
		MsgID:     1,
		Timestamp: 123,
		Payload:   []byte("test"),
	}
	encoded := validEntry.Marshal()
	f.Add(encoded)
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3})
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Try to decode - should either succeed or return error gracefully
		_, err := Unmarshal(bytes.NewReader(data))
		// We don't care if it fails, just that it doesn't panic
		_ = err
	})
}

// FuzzCRC32 tests CRC32 calculation with various inputs
func FuzzCRC32(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte(""))
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 10*1024*1024 {
			t.Skip()
		}

		// Calculate CRC32
		crc := ComputeCRC32C(data)

		// Verify it's deterministic
		crc2 := ComputeCRC32C(data)
		if crc != crc2 {
			t.Errorf("CRC32 is not deterministic: got %d and %d", crc, crc2)
		}

		// Verify different data produces different CRC (most of the time)
		if len(data) > 0 {
			modified := make([]byte, len(data))
			copy(modified, data)
			modified[0] ^= 1 // Flip one bit
			crc3 := ComputeCRC32C(modified)
			// CRC should be different for different data (not always true but likely)
			if len(data) > 4 && crc == crc3 {
				t.Logf("Warning: CRC collision detected (rare but possible)")
			}
		}
	})
}
