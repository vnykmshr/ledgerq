package format

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestEntry_Marshal_Unmarshal_Roundtrip(t *testing.T) {
	tests := []struct {
		name    string
		entry   *Entry
		wantErr bool
	}{
		{
			name: "data entry with payload",
			entry: &Entry{
				Type:      EntryTypeData,
				Flags:     EntryFlagNone,
				MsgID:     12345,
				Timestamp: time.Now().UnixNano(),
				Payload:   []byte("hello, world!"),
			},
		},
		{
			name: "data entry with large payload",
			entry: &Entry{
				Type:      EntryTypeData,
				Flags:     EntryFlagNone,
				MsgID:     67890,
				Timestamp: time.Now().UnixNano(),
				Payload:   bytes.Repeat([]byte("x"), 1024*1024), // 1MB
			},
		},
		{
			name: "tombstone entry",
			entry: &Entry{
				Type:      EntryTypeTombstone,
				Flags:     EntryFlagNone,
				MsgID:     99999,
				Timestamp: time.Now().UnixNano(),
				Payload:   nil,
			},
		},
		{
			name: "checkpoint entry",
			entry: &Entry{
				Type:      EntryTypeCheckpoint,
				Flags:     EntryFlagNone,
				MsgID:     100000,
				Timestamp: time.Now().UnixNano(),
				Payload:   []byte("checkpoint-data"),
			},
		},
		{
			name: "entry with compression flag",
			entry: &Entry{
				Type:      EntryTypeData,
				Flags:     EntryFlagCompressed,
				MsgID:     111111,
				Timestamp: time.Now().UnixNano(),
				Payload:   []byte("compressed-payload"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data := tt.entry.Marshal()

			// Verify total size
			expectedSize := 4 + EntryHeaderSize + len(tt.entry.Payload) + 4
			if len(data) != expectedSize {
				t.Errorf("marshaled size = %d, want %d", len(data), expectedSize)
			}

			// Unmarshal
			reader := bytes.NewReader(data)
			got, err := Unmarshal(reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			// Compare fields
			if got.Type != tt.entry.Type {
				t.Errorf("Type = %d, want %d", got.Type, tt.entry.Type)
			}
			if got.Flags != tt.entry.Flags {
				t.Errorf("Flags = %d, want %d", got.Flags, tt.entry.Flags)
			}
			if got.MsgID != tt.entry.MsgID {
				t.Errorf("MsgID = %d, want %d", got.MsgID, tt.entry.MsgID)
			}
			if got.Timestamp != tt.entry.Timestamp {
				t.Errorf("Timestamp = %d, want %d", got.Timestamp, tt.entry.Timestamp)
			}
			if !bytes.Equal(got.Payload, tt.entry.Payload) {
				t.Errorf("Payload mismatch, got %d bytes, want %d bytes", len(got.Payload), len(tt.entry.Payload))
			}
		})
	}
}

func TestEntry_Unmarshal_CorruptedCRC(t *testing.T) {
	entry := &Entry{
		Type:      EntryTypeData,
		Flags:     EntryFlagNone,
		MsgID:     12345,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test-payload"),
	}

	data := entry.Marshal()

	// Corrupt the data (not the CRC) to cause CRC mismatch
	// Corrupt the MsgID field (bytes 6-13)
	data[10] ^= 0xFF

	reader := bytes.NewReader(data)
	_, err := Unmarshal(reader)
	if err == nil {
		t.Error("Unmarshal() should fail with corrupted data (CRC mismatch)")
	}
	if err != nil && !bytes.Contains([]byte(err.Error()), []byte("CRC mismatch")) {
		t.Errorf("expected CRC mismatch error, got: %v", err)
	}
}

func TestEntry_Unmarshal_CorruptedPayload(t *testing.T) {
	entry := &Entry{
		Type:      EntryTypeData,
		Flags:     EntryFlagNone,
		MsgID:     12345,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test-payload"),
	}

	data := entry.Marshal()

	// Corrupt a byte in the payload
	data[25] ^= 0xFF

	reader := bytes.NewReader(data)
	_, err := Unmarshal(reader)
	if err == nil {
		t.Error("Unmarshal() should fail with corrupted payload")
	}
}

func TestEntry_Unmarshal_InvalidLength(t *testing.T) {
	// Create a buffer with invalid length (too small)
	buf := make([]byte, 8)
	buf[0] = 10 // length = 10, but minimum is 26

	reader := bytes.NewReader(buf)
	_, err := Unmarshal(reader)
	if err == nil {
		t.Error("Unmarshal() should fail with invalid length")
	}
}

func TestEntry_Unmarshal_EOF(t *testing.T) {
	reader := bytes.NewReader([]byte{})
	_, err := Unmarshal(reader)
	if err != io.EOF {
		t.Errorf("Unmarshal() error = %v, want EOF", err)
	}
}

func TestEntry_Unmarshal_TruncatedData(t *testing.T) {
	entry := &Entry{
		Type:      EntryTypeData,
		Flags:     EntryFlagNone,
		MsgID:     12345,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test-payload"),
	}

	data := entry.Marshal()

	// Truncate data
	truncated := data[:len(data)/2]

	reader := bytes.NewReader(truncated)
	_, err := Unmarshal(reader)
	if err == nil {
		t.Error("Unmarshal() should fail with truncated data")
	}
}

func TestEntry_Validate(t *testing.T) {
	now := time.Now().UnixNano()

	tests := []struct {
		name    string
		entry   *Entry
		wantErr bool
	}{
		{
			name: "valid data entry",
			entry: &Entry{
				Length:    uint32(EntryHeaderSize + 5 + 4),
				Type:      EntryTypeData,
				Flags:     EntryFlagNone,
				MsgID:     1,
				Timestamp: now,
				Payload:   []byte("hello"),
			},
			wantErr: false,
		},
		{
			name: "invalid type",
			entry: &Entry{
				Length:    uint32(EntryHeaderSize + 4),
				Type:      99, // invalid
				Flags:     EntryFlagNone,
				MsgID:     1,
				Timestamp: now,
				Payload:   nil,
			},
			wantErr: true,
		},
		{
			name: "zero message ID",
			entry: &Entry{
				Length:    uint32(EntryHeaderSize + 4),
				Type:      EntryTypeData,
				Flags:     EntryFlagNone,
				MsgID:     0, // invalid
				Timestamp: now,
				Payload:   nil,
			},
			wantErr: true,
		},
		{
			name: "invalid timestamp",
			entry: &Entry{
				Length:    uint32(EntryHeaderSize + 4),
				Type:      EntryTypeData,
				Flags:     EntryFlagNone,
				MsgID:     1,
				Timestamp: -1, // invalid
				Payload:   nil,
			},
			wantErr: true,
		},
		{
			name: "length mismatch",
			entry: &Entry{
				Length:    100, // wrong
				Type:      EntryTypeData,
				Flags:     EntryFlagNone,
				MsgID:     1,
				Timestamp: now,
				Payload:   []byte("hello"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEntry_TotalSize(t *testing.T) {
	entry := &Entry{
		Type:      EntryTypeData,
		Flags:     EntryFlagNone,
		MsgID:     12345,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("hello"),
	}

	data := entry.Marshal()
	entry.Length = uint32(len(data) - 4) //nolint:gosec // G115: Test code, safe conversion

	expectedSize := len(data)
	if entry.TotalSize() != expectedSize {
		t.Errorf("TotalSize() = %d, want %d", entry.TotalSize(), expectedSize)
	}
}

// Benchmark tests
func BenchmarkEntry_Marshal(b *testing.B) {
	entry := &Entry{
		Type:      EntryTypeData,
		Flags:     EntryFlagNone,
		MsgID:     12345,
		Timestamp: time.Now().UnixNano(),
		Payload:   bytes.Repeat([]byte("x"), 1024), // 1KB payload
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = entry.Marshal()
	}
}

func BenchmarkEntry_Unmarshal(b *testing.B) {
	entry := &Entry{
		Type:      EntryTypeData,
		Flags:     EntryFlagNone,
		MsgID:     12345,
		Timestamp: time.Now().UnixNano(),
		Payload:   bytes.Repeat([]byte("x"), 1024), // 1KB payload
	}
	data := entry.Marshal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, _ = Unmarshal(reader)
	}
}

func TestEntry_Priority_Marshal_Unmarshal(t *testing.T) {
	tests := []struct {
		name     string
		priority uint8
	}{
		{"low priority (default)", PriorityLow},
		{"medium priority", PriorityMedium},
		{"high priority", PriorityHigh},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &Entry{
				Type:      EntryTypeData,
				MsgID:     12345,
				Timestamp: time.Now().UnixNano(),
				Priority:  tt.priority,
				Payload:   []byte("test-payload"),
			}

			// Marshal
			data := entry.Marshal()

			// Unmarshal
			reader := bytes.NewReader(data)
			got, err := Unmarshal(reader)
			if err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}

			// Verify priority
			if got.Priority != tt.priority {
				t.Errorf("Priority = %d, want %d", got.Priority, tt.priority)
			}

			// Verify flag is set correctly
			if tt.priority == PriorityLow {
				if got.Flags&EntryFlagPriority != 0 {
					t.Error("EntryFlagPriority should not be set for PriorityLow")
				}
			} else {
				if got.Flags&EntryFlagPriority == 0 {
					t.Error("EntryFlagPriority should be set for non-Low priority")
				}
			}
		})
	}
}

func TestEntry_Priority_BackwardCompatibility(t *testing.T) {
	// Entry without priority flag should default to PriorityLow
	entry := &Entry{
		Type:      EntryTypeData,
		Flags:     EntryFlagNone, // No priority flag
		MsgID:     12345,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test"),
	}

	data := entry.Marshal()
	reader := bytes.NewReader(data)
	got, err := Unmarshal(reader)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if got.Priority != PriorityLow {
		t.Errorf("Priority = %d, want %d (default)", got.Priority, PriorityLow)
	}
}

func TestEntry_Priority_WithTTLAndHeaders(t *testing.T) {
	// Test priority combined with TTL and Headers
	now := time.Now().UnixNano()
	entry := &Entry{
		Type:      EntryTypeData,
		MsgID:     12345,
		Timestamp: now,
		Priority:  PriorityHigh,
		ExpiresAt: now + int64(time.Hour),
		Headers: map[string]string{
			"content-type": "application/json",
			"trace-id":     "abc-123",
		},
		Payload: []byte("important data"),
	}

	data := entry.Marshal()
	reader := bytes.NewReader(data)
	got, err := Unmarshal(reader)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if got.Priority != PriorityHigh {
		t.Errorf("Priority = %d, want %d", got.Priority, PriorityHigh)
	}
	if got.ExpiresAt != entry.ExpiresAt {
		t.Errorf("ExpiresAt = %d, want %d", got.ExpiresAt, entry.ExpiresAt)
	}
	if len(got.Headers) != len(entry.Headers) {
		t.Errorf("Headers count = %d, want %d", len(got.Headers), len(entry.Headers))
	}
}

func TestEntry_Priority_Validate(t *testing.T) {
	now := time.Now().UnixNano()

	tests := []struct {
		name     string
		entry    *Entry
		wantErr  bool
		errMatch string
	}{
		{
			name: "valid high priority",
			entry: &Entry{
				Length:    uint32(EntryHeaderSize + 1 + 5 + 4),
				Type:      EntryTypeData,
				Flags:     EntryFlagPriority,
				MsgID:     1,
				Timestamp: now,
				Priority:  PriorityHigh,
				Payload:   []byte("hello"),
			},
			wantErr: false,
		},
		{
			name: "invalid priority value",
			entry: &Entry{
				Length:    uint32(EntryHeaderSize + 1 + 5 + 4),
				Type:      EntryTypeData,
				Flags:     EntryFlagPriority,
				MsgID:     1,
				Timestamp: now,
				Priority:  99, // invalid
				Payload:   []byte("hello"),
			},
			wantErr:  true,
			errMatch: "invalid priority value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errMatch != "" {
				if err == nil || !bytes.Contains([]byte(err.Error()), []byte(tt.errMatch)) {
					t.Errorf("expected error containing %q, got %v", tt.errMatch, err)
				}
			}
		})
	}
}
