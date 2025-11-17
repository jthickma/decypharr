//go:build unix

package vfs

import (
	"os"
	"syscall"
)

// getActualSize returns the actual disk usage of a file
// For sparse files, this returns physical blocks used, not logical size
func getActualSize(path string, info os.FileInfo) int64 {
	if sysInfo, ok := info.Sys().(*syscall.Stat_t); ok {
		// Get actual blocks used on disk (512-byte blocks)
		return int64(sysInfo.Blocks) * 512
	}
	// Fallback to logical size
	return info.Size()
}
