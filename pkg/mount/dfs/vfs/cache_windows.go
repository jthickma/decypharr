//go:build windows

package vfs

import (
	"os"
	"syscall"
)

// getActualSize returns the actual disk usage of a file
// For sparse files on Windows, this uses GetCompressedFileSize
func getActualSize(path string, info os.FileInfo) int64 {
	// Try to get compressed/actual size for sparse files
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return info.Size()
	}

	var high uint32
	low, err := syscall.GetCompressedFileSize(pathPtr, &high)
	if err != nil {
		// Fallback to logical size
		return info.Size()
	}

	// Combine high and low parts
	size := int64(high)<<32 | int64(low)
	return size
}
