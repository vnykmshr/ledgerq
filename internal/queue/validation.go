// Package queue provides validation utilities for queue operations.
// This file contains validation and sanitization functions.
package queue

import (
	"fmt"
	"strings"
	"syscall"
	"time"
)

// checkDiskSpace checks if sufficient disk space is available in the given directory.
// Returns an error if free space is below the configured minimum.
func checkDiskSpace(dir string, minFreeSpace int64) error {
	if minFreeSpace == 0 {
		return nil // Disk space checking disabled
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return fmt.Errorf("failed to check disk space: %w", err)
	}

	// Available blocks * block size = available bytes
	availableBytes := int64(stat.Bavail * uint64(stat.Bsize))

	if availableBytes < minFreeSpace {
		return fmt.Errorf("insufficient disk space: %d bytes available, %d bytes required",
			availableBytes, minFreeSpace)
	}

	return nil
}

// validateMessageSize checks if the message payload exceeds the maximum allowed size.
// Returns an error if the payload is too large.
func validateMessageSize(payload []byte, maxSize int64) error {
	if maxSize == 0 {
		return nil // No size limit
	}

	if int64(len(payload)) > maxSize {
		return fmt.Errorf("message size %d bytes exceeds maximum %d bytes",
			len(payload), maxSize)
	}

	return nil
}

// sanitizeFailureReason sanitizes failure reason strings to prevent information leakage.
// Removes stack traces and truncates to a maximum length.
func sanitizeFailureReason(reason string) string {
	const maxLength = 256

	// Remove stack traces (lines starting with common stack trace patterns)
	lines := strings.Split(reason, "\n")
	sanitized := lines[0] // Keep only the first line (the actual error message)

	// Truncate if too long
	if len(sanitized) > maxLength {
		sanitized = sanitized[:maxLength-3] + "..."
	}

	return sanitized
}

// CalculateBackoff calculates exponential backoff duration based on retry count (v1.2.0+).
// This is a helper function for implementing retry logic with the DLQ system.
// The backoff duration increases exponentially: base * 2^retryCount, capped at maxBackoff.
//
// Parameters:
//   - retryCount: Number of previous retry attempts (typically from RetryInfo.RetryCount)
//   - baseDelay: Base delay for the first retry (e.g., 1 second)
//   - maxBackoff: Maximum backoff duration to prevent excessively long waits
//
// Example usage:
//
//	msg, _ := q.Dequeue()
//	if info := q.GetRetryInfo(msg.ID); info != nil && info.RetryCount > 0 {
//	    backoff := CalculateBackoff(info.RetryCount, time.Second, 5*time.Minute)
//	    time.Sleep(backoff)
//	}
//
// Returns the calculated backoff duration, always between baseDelay and maxBackoff.
func CalculateBackoff(retryCount int, baseDelay, maxBackoff time.Duration) time.Duration {
	if retryCount <= 0 {
		return baseDelay
	}

	// Calculate exponential backoff: baseDelay * 2^retryCount
	// Use bit shifting for efficiency: 1 << retryCount = 2^retryCount
	multiplier := int64(1 << uint(retryCount))

	// Prevent overflow by checking if multiplier would be too large
	maxMultiplier := int64(maxBackoff / baseDelay)
	if multiplier > maxMultiplier {
		multiplier = maxMultiplier
	}

	backoff := time.Duration(multiplier) * baseDelay

	// Ensure we don't exceed the maximum backoff
	if backoff > maxBackoff {
		return maxBackoff
	}

	// Ensure we don't go below the base delay
	if backoff < baseDelay {
		return baseDelay
	}

	return backoff
}
