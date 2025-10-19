package format

import (
	"sort"
	"sync"
)

// PriorityIndex maintains separate indices for each priority level.
// This allows efficient O(log n) lookups within each priority tier
// while maintaining FIFO order within the same priority.
type PriorityIndex struct {
	mu sync.RWMutex

	// Separate slices for each priority level (sorted by offset)
	high   []PriorityIndexEntry
	medium []PriorityIndexEntry
	low    []PriorityIndexEntry

	// Map for O(1) offset lookup
	byOffset map[uint64]*PriorityIndexEntry
}

// PriorityIndexEntry represents a single entry in the priority index.
type PriorityIndexEntry struct {
	Offset    uint64 // Message offset
	Position  uint32 // Position in segment file
	Priority  uint8  // Priority level
	Timestamp int64  // For starvation prevention
}

// NewPriorityIndex creates a new empty priority index.
func NewPriorityIndex() *PriorityIndex {
	return &PriorityIndex{
		high:     make([]PriorityIndexEntry, 0, 1024),
		medium:   make([]PriorityIndexEntry, 0, 1024),
		low:      make([]PriorityIndexEntry, 0, 4096),
		byOffset: make(map[uint64]*PriorityIndexEntry),
	}
}

// Insert adds a new entry to the appropriate priority slice.
func (pi *PriorityIndex) Insert(offset uint64, position uint32, priority uint8, timestamp int64) {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	entry := PriorityIndexEntry{
		Offset:    offset,
		Position:  position,
		Priority:  priority,
		Timestamp: timestamp,
	}

	// Add to priority-specific slice
	switch priority {
	case PriorityHigh:
		pi.high = append(pi.high, entry)
	case PriorityMedium:
		pi.medium = append(pi.medium, entry)
	default: // PriorityLow
		pi.low = append(pi.low, entry)
	}

	// Add to offset map (pointer to the slice element)
	// Note: We store pointer to the last element added
	var sliceEntry *PriorityIndexEntry
	switch priority {
	case PriorityHigh:
		sliceEntry = &pi.high[len(pi.high)-1]
	case PriorityMedium:
		sliceEntry = &pi.medium[len(pi.medium)-1]
	default:
		sliceEntry = &pi.low[len(pi.low)-1]
	}
	pi.byOffset[offset] = sliceEntry
}

// NextInPriority returns the first offset in the given priority level
// that is greater than afterOffset. Returns (0, false) if not found.
func (pi *PriorityIndex) NextInPriority(priority uint8, afterOffset uint64) (uint64, bool) {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	var slice []PriorityIndexEntry
	switch priority {
	case PriorityHigh:
		slice = pi.high
	case PriorityMedium:
		slice = pi.medium
	default:
		slice = pi.low
	}

	// Binary search for first entry > afterOffset
	idx := sort.Search(len(slice), func(i int) bool {
		return slice[i].Offset > afterOffset
	})

	if idx < len(slice) {
		return slice[idx].Offset, true
	}
	return 0, false
}

// OldestInPriority returns the oldest (first) entry in the given priority level.
// Returns nil if the priority level is empty.
func (pi *PriorityIndex) OldestInPriority(priority uint8) *PriorityIndexEntry {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	var slice []PriorityIndexEntry
	switch priority {
	case PriorityHigh:
		slice = pi.high
	case PriorityMedium:
		slice = pi.medium
	default:
		slice = pi.low
	}

	if len(slice) == 0 {
		return nil
	}
	return &slice[0]
}

// Get retrieves an entry by offset. Returns nil if not found.
func (pi *PriorityIndex) Get(offset uint64) *PriorityIndexEntry {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	return pi.byOffset[offset]
}

// Remove removes an entry from the index by offset.
func (pi *PriorityIndex) Remove(offset uint64) {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	entry, ok := pi.byOffset[offset]
	if !ok {
		return
	}

	// Remove from priority-specific slice
	var slice *[]PriorityIndexEntry
	switch entry.Priority {
	case PriorityHigh:
		slice = &pi.high
	case PriorityMedium:
		slice = &pi.medium
	default:
		slice = &pi.low
	}

	// Binary search and remove
	idx := sort.Search(len(*slice), func(i int) bool {
		return (*slice)[i].Offset >= offset
	})

	if idx < len(*slice) && (*slice)[idx].Offset == offset {
		*slice = append((*slice)[:idx], (*slice)[idx+1:]...)
	}

	delete(pi.byOffset, offset)
}

// Count returns the total number of entries across all priorities.
func (pi *PriorityIndex) Count() int {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	return len(pi.high) + len(pi.medium) + len(pi.low)
}

// CountByPriority returns the number of entries for a specific priority.
func (pi *PriorityIndex) CountByPriority(priority uint8) int {
	pi.mu.RLock()
	defer pi.mu.RUnlock()

	switch priority {
	case PriorityHigh:
		return len(pi.high)
	case PriorityMedium:
		return len(pi.medium)
	default:
		return len(pi.low)
	}
}

// Clear removes all entries from the index.
func (pi *PriorityIndex) Clear() {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	pi.high = pi.high[:0]
	pi.medium = pi.medium[:0]
	pi.low = pi.low[:0]
	pi.byOffset = make(map[uint64]*PriorityIndexEntry)
}
