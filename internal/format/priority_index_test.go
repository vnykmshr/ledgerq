package format

import (
	"testing"
	"time"
)

func TestPriorityIndex_Insert_And_Get(t *testing.T) {
	pi := NewPriorityIndex()

	// Insert entries with different priorities
	pi.Insert(100, 0, PriorityLow, time.Now().UnixNano())
	pi.Insert(200, 1024, PriorityMedium, time.Now().UnixNano())
	pi.Insert(300, 2048, PriorityHigh, time.Now().UnixNano())

	// Verify counts
	if pi.Count() != 3 {
		t.Errorf("Count() = %d, want 3", pi.Count())
	}

	// Verify Get
	entry := pi.Get(200)
	if entry == nil {
		t.Fatal("Get(200) returned nil")
	}
	if entry.Offset != 200 {
		t.Errorf("entry.Offset = %d, want 200", entry.Offset)
	}
	if entry.Priority != PriorityMedium {
		t.Errorf("entry.Priority = %d, want %d", entry.Priority, PriorityMedium)
	}
}

func TestPriorityIndex_NextInPriority(t *testing.T) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Insert entries in different priorities
	pi.Insert(100, 0, PriorityLow, now)
	pi.Insert(200, 1024, PriorityLow, now)
	pi.Insert(150, 512, PriorityMedium, now)
	pi.Insert(250, 1536, PriorityMedium, now)
	pi.Insert(175, 768, PriorityHigh, now)
	pi.Insert(275, 2048, PriorityHigh, now)

	tests := []struct {
		name        string
		priority    uint8
		afterOffset uint64
		wantOffset  uint64
		wantOK      bool
	}{
		{
			name:        "high priority - first",
			priority:    PriorityHigh,
			afterOffset: 0,
			wantOffset:  175,
			wantOK:      true,
		},
		{
			name:        "high priority - second",
			priority:    PriorityHigh,
			afterOffset: 175,
			wantOffset:  275,
			wantOK:      true,
		},
		{
			name:        "high priority - none remaining",
			priority:    PriorityHigh,
			afterOffset: 275,
			wantOffset:  0,
			wantOK:      false,
		},
		{
			name:        "medium priority - first",
			priority:    PriorityMedium,
			afterOffset: 0,
			wantOffset:  150,
			wantOK:      true,
		},
		{
			name:        "low priority - skip first",
			priority:    PriorityLow,
			afterOffset: 100,
			wantOffset:  200,
			wantOK:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := pi.NextInPriority(tt.priority, tt.afterOffset)
			if ok != tt.wantOK {
				t.Errorf("NextInPriority() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.wantOffset {
				t.Errorf("NextInPriority() = %d, want %d", got, tt.wantOffset)
			}
		})
	}
}

func TestPriorityIndex_OldestInPriority(t *testing.T) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Insert entries (out of order within priority)
	pi.Insert(300, 2048, PriorityHigh, now)
	pi.Insert(100, 0, PriorityHigh, now)
	pi.Insert(200, 1024, PriorityHigh, now)

	// Oldest should be offset 100 (first in slice)
	oldest := pi.OldestInPriority(PriorityHigh)
	if oldest == nil {
		t.Fatal("OldestInPriority() returned nil")
	}
	if oldest.Offset != 300 { // First inserted
		t.Errorf("OldestInPriority() offset = %d, want 300", oldest.Offset)
	}

	// Empty priority should return nil
	emptyOldest := pi.OldestInPriority(PriorityMedium)
	if emptyOldest != nil {
		t.Error("OldestInPriority() for empty priority should return nil")
	}
}

func TestPriorityIndex_Remove(t *testing.T) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Insert entries
	pi.Insert(100, 0, PriorityLow, now)
	pi.Insert(200, 1024, PriorityMedium, now)
	pi.Insert(300, 2048, PriorityHigh, now)

	if pi.Count() != 3 {
		t.Fatalf("Count() = %d, want 3", pi.Count())
	}

	// Remove middle entry
	pi.Remove(200)

	if pi.Count() != 2 {
		t.Errorf("Count() after Remove = %d, want 2", pi.Count())
	}

	// Verify it's gone
	if entry := pi.Get(200); entry != nil {
		t.Error("Get(200) should return nil after Remove")
	}

	// Verify others still exist
	if entry := pi.Get(100); entry == nil {
		t.Error("Get(100) should still exist")
	}
	if entry := pi.Get(300); entry == nil {
		t.Error("Get(300) should still exist")
	}

	// Remove non-existent entry (should not panic)
	pi.Remove(999)
	if pi.Count() != 2 {
		t.Errorf("Count() after removing non-existent = %d, want 2", pi.Count())
	}
}

func TestPriorityIndex_CountByPriority(t *testing.T) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Insert various priorities
	pi.Insert(100, 0, PriorityLow, now)
	pi.Insert(200, 1024, PriorityLow, now)
	pi.Insert(300, 2048, PriorityMedium, now)
	pi.Insert(400, 3072, PriorityHigh, now)
	pi.Insert(500, 4096, PriorityHigh, now)
	pi.Insert(600, 5120, PriorityHigh, now)

	tests := []struct {
		priority uint8
		want     int
	}{
		{PriorityLow, 2},
		{PriorityMedium, 1},
		{PriorityHigh, 3},
	}

	for _, tt := range tests {
		got := pi.CountByPriority(tt.priority)
		if got != tt.want {
			t.Errorf("CountByPriority(%d) = %d, want %d", tt.priority, got, tt.want)
		}
	}
}

func TestPriorityIndex_Clear(t *testing.T) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Insert entries
	pi.Insert(100, 0, PriorityLow, now)
	pi.Insert(200, 1024, PriorityMedium, now)
	pi.Insert(300, 2048, PriorityHigh, now)

	if pi.Count() != 3 {
		t.Fatalf("Count() = %d, want 3", pi.Count())
	}

	// Clear
	pi.Clear()

	if pi.Count() != 0 {
		t.Errorf("Count() after Clear() = %d, want 0", pi.Count())
	}

	// Verify all priorities are empty
	if pi.CountByPriority(PriorityLow) != 0 {
		t.Error("PriorityLow should be empty after Clear()")
	}
	if pi.CountByPriority(PriorityMedium) != 0 {
		t.Error("PriorityMedium should be empty after Clear()")
	}
	if pi.CountByPriority(PriorityHigh) != 0 {
		t.Error("PriorityHigh should be empty after Clear()")
	}
}

func TestPriorityIndex_FIFOWithinPriority(t *testing.T) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Insert multiple high priority messages
	offsets := []uint64{100, 150, 200, 250, 300}
	for i, offset := range offsets {
		pi.Insert(offset, uint32(i*1024), PriorityHigh, now) //nolint:gosec // G115: Test code
	}

	// Verify FIFO order: NextInPriority should return them in insertion order
	afterOffset := uint64(0)
	for _, expectedOffset := range offsets {
		gotOffset, ok := pi.NextInPriority(PriorityHigh, afterOffset)
		if !ok {
			t.Fatalf("NextInPriority() returned false for offset %d", expectedOffset)
		}
		if gotOffset != expectedOffset {
			t.Errorf("NextInPriority() = %d, want %d", gotOffset, expectedOffset)
		}
		afterOffset = gotOffset
	}

	// No more entries
	_, ok := pi.NextInPriority(PriorityHigh, afterOffset)
	if ok {
		t.Error("NextInPriority() should return false when no more entries")
	}
}

func TestPriorityIndex_ConcurrentAccess(t *testing.T) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Concurrent inserts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(offset uint64) {
			pi.Insert(offset, uint32(offset), PriorityHigh, now) //nolint:gosec // G115: Test code
			done <- true
		}(uint64((i + 1) * 100))
	}

	// Wait for all inserts
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all were inserted
	if pi.Count() != 10 {
		t.Errorf("Count() = %d, want 10", pi.Count())
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func(offset uint64) {
			entry := pi.Get(offset)
			if entry == nil {
				t.Errorf("Get(%d) returned nil", offset)
			}
			done <- true
		}(uint64((i + 1) * 100))
	}

	// Wait for all reads
	for i := 0; i < 10; i++ {
		<-done
	}
}

func BenchmarkPriorityIndex_Insert(b *testing.B) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pi.Insert(uint64(i), uint32(i*1024), PriorityMedium, now) //nolint:gosec // G115: Test code
	}
}

func BenchmarkPriorityIndex_NextInPriority(b *testing.B) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Pre-populate with 10000 entries
	for i := 0; i < 10000; i++ {
		pi.Insert(uint64(i*100), uint32(i*1024), PriorityMedium, now) //nolint:gosec // G115: Test code
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint64((i % 10000) * 100)
		pi.NextInPriority(PriorityMedium, offset)
	}
}

func BenchmarkPriorityIndex_Get(b *testing.B) {
	pi := NewPriorityIndex()
	now := time.Now().UnixNano()

	// Pre-populate with 10000 entries
	for i := 0; i < 10000; i++ {
		pi.Insert(uint64(i*100), uint32(i*1024), PriorityMedium, now) //nolint:gosec // G115: Test code
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint64((i % 10000) * 100)
		pi.Get(offset)
	}
}
