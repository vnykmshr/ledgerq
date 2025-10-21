// Package queue provides deduplication tracking for message idempotency.
package queue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DedupTracker manages hash-based deduplication with time windows.
// It provides O(1) duplicate detection using SHA-256 hashing and
// maintains crash-safe persistent state.
type DedupTracker struct {
	mu        sync.RWMutex
	entries   map[string]*dedupEntry
	maxSize   int
	statePath string
}

// dedupEntry represents a tracked message for deduplication.
type dedupEntry struct {
	DedupID    string `json:"dedup_id"`    // SHA-256 hash of user-provided ID
	MessageID  uint64 `json:"message_id"`  // Original message ID
	EnqueuedAt int64  `json:"enqueued_at"` // Unix nanoseconds
	ExpiresAt  int64  `json:"expires_at"`  // Unix nanoseconds
}

// dedupState represents the persistent state format.
type dedupState struct {
	Version int           `json:"version"`
	Entries []*dedupEntry `json:"entries"`
}

const dedupStateVersion = 1

// NewDedupTracker creates a new deduplication tracker.
func NewDedupTracker(statePath string, maxSize int) *DedupTracker {
	if maxSize <= 0 {
		maxSize = 100000 // Default: 100K entries
	}

	return &DedupTracker{
		entries:   make(map[string]*dedupEntry),
		maxSize:   maxSize,
		statePath: statePath,
	}
}

// Check returns the original message ID if the dedup ID is a duplicate.
// Returns (msgID, true) if duplicate found, (0, false) if new.
func (dt *DedupTracker) Check(dedupID string, window time.Duration) (uint64, bool) {
	if dedupID == "" {
		return 0, false
	}

	dt.mu.RLock()
	defer dt.mu.RUnlock()

	hash := hashDedupID(dedupID)
	entry, exists := dt.entries[hash]
	if !exists {
		return 0, false
	}

	// Check if entry has expired
	now := time.Now().UnixNano()
	if now >= entry.ExpiresAt {
		return 0, false
	}

	return entry.MessageID, true
}

// Track adds a new message to the deduplication table.
// Returns error if table is full or dedup ID is empty.
func (dt *DedupTracker) Track(dedupID string, msgID uint64, window time.Duration) error {
	if dedupID == "" {
		return fmt.Errorf("deduplication ID cannot be empty")
	}

	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Check size limit (exclude expired entries from count)
	activeCount := dt.countActiveEntriesLocked(time.Now().UnixNano())
	if activeCount >= dt.maxSize {
		return fmt.Errorf("deduplication table full (max: %d entries)", dt.maxSize)
	}

	hash := hashDedupID(dedupID)
	now := time.Now().UnixNano()

	dt.entries[hash] = &dedupEntry{
		DedupID:    hash,
		MessageID:  msgID,
		EnqueuedAt: now,
		ExpiresAt:  now + window.Nanoseconds(),
	}

	return nil
}

// CleanExpired removes all expired entries from the table.
// Returns the number of entries removed.
func (dt *DedupTracker) CleanExpired() int {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	now := time.Now().UnixNano()
	removed := 0

	for hash, entry := range dt.entries {
		if now >= entry.ExpiresAt {
			delete(dt.entries, hash)
			removed++
		}
	}

	return removed
}

// Count returns the total number of entries (including expired).
func (dt *DedupTracker) Count() int {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	return len(dt.entries)
}

// ActiveCount returns the number of non-expired entries.
func (dt *DedupTracker) ActiveCount() int {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	return dt.countActiveEntriesLocked(time.Now().UnixNano())
}

// countActiveEntriesLocked counts non-expired entries (must hold lock).
func (dt *DedupTracker) countActiveEntriesLocked(now int64) int {
	count := 0
	for _, entry := range dt.entries {
		if now < entry.ExpiresAt {
			count++
		}
	}
	return count
}

// Persist saves the deduplication state to disk using atomic writes.
// Uses temp file + rename pattern for crash safety.
func (dt *DedupTracker) Persist() error {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	// Collect all entries
	entries := make([]*dedupEntry, 0, len(dt.entries))
	for _, entry := range dt.entries {
		entries = append(entries, entry)
	}

	state := &dedupState{
		Version: dedupStateVersion,
		Entries: entries,
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal dedup state: %w", err)
	}

	// Write to temp file
	tmpPath := dt.statePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write temp state file: %w", err)
	}

	// Sync to ensure data is on disk
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to open temp file for sync: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	f.Close()

	// Atomic rename
	if err := os.Rename(tmpPath, dt.statePath); err != nil {
		return fmt.Errorf("failed to rename state file: %w", err)
	}

	return nil
}

// Load restores the deduplication state from disk.
// Returns nil if state file doesn't exist (fresh start).
func (dt *DedupTracker) Load() error {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Check if state file exists
	if _, err := os.Stat(dt.statePath); os.IsNotExist(err) {
		return nil // Fresh start, no state to load
	}

	// Read state file
	data, err := os.ReadFile(dt.statePath)
	if err != nil {
		return fmt.Errorf("failed to read dedup state: %w", err)
	}

	// Unmarshal JSON
	var state dedupState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal dedup state: %w", err)
	}

	// Validate version
	if state.Version != dedupStateVersion {
		return fmt.Errorf("unsupported dedup state version: %d (expected %d)",
			state.Version, dedupStateVersion)
	}

	// Restore entries (skip expired ones)
	now := time.Now().UnixNano()
	for _, entry := range state.Entries {
		if now < entry.ExpiresAt {
			dt.entries[entry.DedupID] = entry
		}
	}

	return nil
}

// Close persists state and cleans up resources.
func (dt *DedupTracker) Close() error {
	return dt.Persist()
}

// hashDedupID computes SHA-256 hash of deduplication ID.
func hashDedupID(dedupID string) string {
	h := sha256.Sum256([]byte(dedupID))
	return hex.EncodeToString(h[:])
}

// EnsureStateDir creates the state directory if it doesn't exist.
func (dt *DedupTracker) EnsureStateDir() error {
	dir := filepath.Dir(dt.statePath)
	return os.MkdirAll(dir, 0700)
}
