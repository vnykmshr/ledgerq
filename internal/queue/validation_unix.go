//go:build unix || linux || darwin || freebsd || openbsd || netbsd

// Package queue provides validation utilities for queue operations.
// This file contains Unix-specific disk space checking functionality.
package queue

import (
	"fmt"
	"syscall"
)

// checkDiskSpace checks if sufficient disk space is available in the given directory.
// Returns an error if free space is below the configured minimum.
// This implementation uses Unix-specific syscalls (Statfs).
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
