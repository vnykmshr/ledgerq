//go:build windows

// Package queue provides validation utilities for queue operations.
// This file contains Windows-specific disk space checking functionality.
package queue

import (
	"fmt"
	"syscall"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpace = kernel32.NewProc("GetDiskFreeSpaceExW")
)

// checkDiskSpace checks if sufficient disk space is available in the given directory.
// Returns an error if free space is below the configured minimum.
// This implementation uses Windows-specific API (GetDiskFreeSpaceExW).
func checkDiskSpace(dir string, minFreeSpace int64) error {
	if minFreeSpace == 0 {
		return nil // Disk space checking disabled
	}

	// Convert path to UTF-16 for Windows API
	dirUTF16, err := syscall.UTF16PtrFromString(dir)
	if err != nil {
		return fmt.Errorf("failed to convert path: %w", err)
	}

	var freeBytesAvailable uint64
	var totalBytes uint64
	var totalFreeBytes uint64

	// Call GetDiskFreeSpaceExW
	ret, _, err := getDiskFreeSpace.Call(
		uintptr(unsafe.Pointer(dirUTF16)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&totalFreeBytes)),
	)

	if ret == 0 {
		return fmt.Errorf("failed to check disk space: %w", err)
	}

	availableBytes := int64(freeBytesAvailable)

	if availableBytes < minFreeSpace {
		return fmt.Errorf("insufficient disk space: %d bytes available, %d bytes required",
			availableBytes, minFreeSpace)
	}

	return nil
}
