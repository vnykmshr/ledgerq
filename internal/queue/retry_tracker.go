package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// RetryTracker manages message retry state for Dead Letter Queue functionality.
// It tracks how many times each message has failed processing and stores
// failure metadata for debugging purposes.
//
// The retry state is persisted to disk as JSON for crash recovery.
type RetryTracker struct {
	path       string                // Path to retry state file
	maxRetries int                   // Maximum retry attempts before DLQ
	entries    map[uint64]*RetryInfo // Message ID -> retry information
	mu         sync.RWMutex          // Protects entries map
}

// RetryInfo contains retry metadata for a single message.
type RetryInfo struct {
	MessageID     uint64    `json:"msg_id"`
	RetryCount    int       `json:"retry_count"`
	LastFailure   time.Time `json:"last_failure"`
	FailureReason string    `json:"failure_reason,omitempty"`
}

// retryState is the on-disk format for retry tracking.
type retryState struct {
	Version    int          `json:"version"`
	MaxRetries int          `json:"max_retries"`
	Entries    []*RetryInfo `json:"entries"`
}

const retryStateVersion = 1

// NewRetryTracker creates a new retry tracker.
// If a retry state file exists at the given path, it will be loaded.
// Otherwise, a new tracker is created.
func NewRetryTracker(path string, maxRetries int) (*RetryTracker, error) {
	if maxRetries < 0 {
		return nil, fmt.Errorf("max retries must be >= 0, got %d", maxRetries)
	}

	rt := &RetryTracker{
		path:       path,
		maxRetries: maxRetries,
		entries:    make(map[uint64]*RetryInfo),
	}

	// Load existing state if present
	if err := rt.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load retry state: %w", err)
	}

	return rt, nil
}

// Nack records a message processing failure.
// It increments the retry count and stores the failure reason.
// Returns true if the message has exceeded max retries and should move to DLQ.
func (rt *RetryTracker) Nack(msgID uint64, reason string) (bool, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Get or create retry entry
	entry, exists := rt.entries[msgID]
	if !exists {
		entry = &RetryInfo{
			MessageID: msgID,
		}
		rt.entries[msgID] = entry
	}

	// Update retry information
	entry.RetryCount++
	entry.LastFailure = time.Now()
	entry.FailureReason = reason

	// Persist state to disk
	if err := rt.save(); err != nil {
		// Rollback in-memory state on persistence failure
		entry.RetryCount--
		return false, fmt.Errorf("failed to persist retry state: %w", err)
	}

	// Check if max retries exceeded (0 = unlimited retries)
	exceeded := rt.maxRetries > 0 && entry.RetryCount >= rt.maxRetries

	return exceeded, nil
}

// Ack removes retry tracking for a successfully processed message.
// This is called when a message is processed without errors.
func (rt *RetryTracker) Ack(msgID uint64) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Remove from tracking
	delete(rt.entries, msgID)

	// Persist state (clean up)
	return rt.save()
}

// GetInfo returns retry information for a message.
// Returns nil if no retry information exists (message hasn't failed yet).
func (rt *RetryTracker) GetInfo(msgID uint64) *RetryInfo {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	entry := rt.entries[msgID]
	if entry == nil {
		return nil
	}

	// Return a copy to prevent external modifications
	return &RetryInfo{
		MessageID:     entry.MessageID,
		RetryCount:    entry.RetryCount,
		LastFailure:   entry.LastFailure,
		FailureReason: entry.FailureReason,
	}
}

// Count returns the number of messages currently being tracked.
// This indicates how many messages have active retry state.
func (rt *RetryTracker) Count() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return len(rt.entries)
}

// Close persists the final state and cleans up resources.
func (rt *RetryTracker) Close() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.save()
}

// load reads retry state from disk.
func (rt *RetryTracker) load() error {
	data, err := os.ReadFile(rt.path)
	if err != nil {
		return err
	}

	var state retryState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("invalid retry state format: %w", err)
	}

	// Validate version
	if state.Version != retryStateVersion {
		return fmt.Errorf("unsupported retry state version: %d (expected %d)",
			state.Version, retryStateVersion)
	}

	// Load entries into map
	for _, entry := range state.Entries {
		rt.entries[entry.MessageID] = entry
	}

	return nil
}

// save writes retry state to disk atomically.
// Uses a temp file + rename pattern for crash safety.
func (rt *RetryTracker) save() error {
	// Convert map to slice for JSON serialization
	entries := make([]*RetryInfo, 0, len(rt.entries))
	for _, entry := range rt.entries {
		entries = append(entries, entry)
	}

	state := retryState{
		Version:    retryStateVersion,
		MaxRetries: rt.maxRetries,
		Entries:    entries,
	}

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal retry state: %w", err)
	}

	// Write to temporary file first with restrictive permissions (owner-only read/write)
	tmpPath := rt.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, rt.path); err != nil {
		_ = os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// Clear removes all retry tracking state.
// This is useful for testing or manual cleanup.
func (rt *RetryTracker) Clear() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.entries = make(map[uint64]*RetryInfo)
	return rt.save()
}
