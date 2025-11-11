package dfs

import "sync/atomic"

// DFSStats provides unified statistics across all DFS mounts
// This aggregates stats from all mounted filesystems into a single view
type DFSStats struct {
	// Disk cache statistics
	CacheDirSize  atomic.Int64 // Total bytes used across all mounts
	CacheDirLimit atomic.Int64 // Total cache limit across all mounts

	// File operations
	OpenedFiles atomic.Int64 // Total files currently open
	ActiveReads atomic.Int64 // Total active read operations

	// Memory buffer statistics
	MemoryUsed     atomic.Int64 // Total memory buffer usage (bytes)
	MemoryLimit    atomic.Int64 // Total memory buffer limit (bytes)
	MemHits        atomic.Int64 // Total memory cache hits
	MemMisses      atomic.Int64 // Total memory cache misses
	MemEvictions   atomic.Int64 // Total memory evictions
	MemFlushes     atomic.Int64 // Total async flushes
	MemFlushBytes  atomic.Int64 // Total bytes flushed
	MemChunksCount atomic.Int64 // Total chunks in memory
	MemFilesCount  atomic.Int64 // Total files with memory buffers

	// Configuration (same across all mounts)
	ChunkSize     int64
	ReadAheadSize int64
	BufferSize    int64
}

// NewDFSStats creates a new stats tracker
func NewDFSStats() *DFSStats {
	return &DFSStats{}
}

// Reset resets all statistics to zero
func (s *DFSStats) Reset() {
	s.CacheDirSize.Store(0)
	s.CacheDirLimit.Store(0)
	s.OpenedFiles.Store(0)
	s.ActiveReads.Store(0)
	s.MemoryUsed.Store(0)
	s.MemoryLimit.Store(0)
	s.MemHits.Store(0)
	s.MemMisses.Store(0)
	s.MemEvictions.Store(0)
	s.MemFlushes.Store(0)
	s.MemFlushBytes.Store(0)
	s.MemChunksCount.Store(0)
	s.MemFilesCount.Store(0)
}

// ToMap converts stats to a map for JSON serialization
func (s *DFSStats) ToMap() map[string]interface{} {
	stats := map[string]interface{}{
		"cache_dir_size":  s.CacheDirSize.Load(),
		"cache_dir_limit": s.CacheDirLimit.Load(),
		"active_reads":    s.ActiveReads.Load(),
		"opened_files":    s.OpenedFiles.Load(),
		"chunk_size":      s.ChunkSize,
		"read_ahead_size": s.ReadAheadSize,
		"buffer_size":     s.BufferSize,
	}

	// Add memory buffer stats if any files have buffers
	if s.MemFilesCount.Load() > 0 {
		hitRate := 0.0
		total := s.MemHits.Load() + s.MemMisses.Load()
		if total > 0 {
			hitRate = float64(s.MemHits.Load()) / float64(total) * 100.0
		}

		stats["memory_buffer"] = map[string]interface{}{
			"hits":         s.MemHits.Load(),
			"misses":       s.MemMisses.Load(),
			"hit_rate_pct": hitRate,
			"evictions":    s.MemEvictions.Load(),
			"flushes":      s.MemFlushes.Load(),
			"flush_bytes":  s.MemFlushBytes.Load(),
			"memory_used":  s.MemoryUsed.Load(),
			"memory_limit": s.MemoryLimit.Load(),
			"chunks_count": s.MemChunksCount.Load(),
			"files_count":  s.MemFilesCount.Load(),
		}
	}

	return stats
}

// MountStats represents statistics for a single mount
type MountStats struct {
	Name      string
	Type      string
	Mounted   bool
	MountPath string

	// Cache
	CacheDirSize  int64
	CacheDirLimit int64

	// Operations
	OpenedFiles int64
	ActiveReads int64

	// Memory buffer (optional, only if files are open)
	MemoryBuffer *MemoryBufferStats
}

// MemoryBufferStats represents memory buffer statistics
type MemoryBufferStats struct {
	Hits        int64
	Misses      int64
	HitRatePct  float64
	Evictions   int64
	Flushes     int64
	FlushBytes  int64
	MemoryUsed  int64
	MemoryLimit int64
	ChunksCount int64
	FilesCount  int
}

// ToMap converts mount stats to map
func (m *MountStats) ToMap() map[string]interface{} {
	stats := map[string]interface{}{
		"name":            m.Name,
		"type":            m.Type,
		"mounted":         m.Mounted,
		"mount_path":      m.MountPath,
		"cache_dir_size":  m.CacheDirSize,
		"cache_dir_limit": m.CacheDirLimit,
		"active_reads":    m.ActiveReads,
		"opened_files":    m.OpenedFiles,
	}

	if m.MemoryBuffer != nil {
		stats["memory_buffer"] = map[string]interface{}{
			"hits":         m.MemoryBuffer.Hits,
			"misses":       m.MemoryBuffer.Misses,
			"hit_rate_pct": m.MemoryBuffer.HitRatePct,
			"evictions":    m.MemoryBuffer.Evictions,
			"flushes":      m.MemoryBuffer.Flushes,
			"flush_bytes":  m.MemoryBuffer.FlushBytes,
			"memory_used":  m.MemoryBuffer.MemoryUsed,
			"memory_limit": m.MemoryBuffer.MemoryLimit,
			"chunks_count": m.MemoryBuffer.ChunksCount,
			"files_count":  m.MemoryBuffer.FilesCount,
		}
	}

	return stats
}
