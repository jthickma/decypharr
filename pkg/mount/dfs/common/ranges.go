package common

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// NumRangeShards Number of shards - power of 2 for fast modulo
	NumRangeShards = 64
	ShardMask      = NumRangeShards - 1

	// ShardChunkSize Chunk size for shard distribution (1MB)
	ShardChunkSize = 1024 * 1024

	// SnapshotUpdateInterval Snapshot update interval
	SnapshotUpdateInterval = 5 * time.Second
)

// Range represents a contiguous range of data
type Range struct {
	Pos  int64
	Size int64
}

// End returns the end position of the range
func (r Range) End() int64 {
	return r.Pos + r.Size
}

// Overlaps checks if two ranges overlap
func (r Range) Overlaps(other Range) bool {
	return r.Pos < other.End() && other.Pos < r.End()
}

// Contains checks if this range fully contains another range
func (r Range) Contains(other Range) bool {
	return r.Pos <= other.Pos && r.End() >= other.End()
}

// Intersect returns the intersection of two ranges, or zero range if no intersection
func (r Range) Intersect(other Range) Range {
	start := max(r.Pos, other.Pos)
	end := min(r.End(), other.End())
	if start >= end {
		return Range{} // No intersection
	}
	return Range{Pos: start, Size: end - start}
}

// RangeShard holds ranges for a specific shard
type RangeShard struct {
	mu     sync.RWMutex
	ranges []Range
}

// Insert adds a range to the shard, merging with existing ranges if needed
func (rs *RangeShard) Insert(r Range) {
	if r.Size <= 0 {
		return
	}

	// Find insertion point and merge overlapping ranges
	var merged []Range
	inserted := false

	for _, existing := range rs.ranges {
		if !inserted && (r.Overlaps(existing) || r.End() == existing.Pos || existing.End() == r.Pos) {
			// Merge ranges
			start := min(r.Pos, existing.Pos)
			end := max(r.End(), existing.End())
			r = Range{Pos: start, Size: end - start}
		} else if !inserted && r.End() < existing.Pos {
			// Insert before this range
			merged = append(merged, r)
			merged = append(merged, existing)
			inserted = true
		} else if inserted || r.Pos > existing.End() {
			// Add existing range
			merged = append(merged, existing)
		} else {
			// Continue merging
			start := min(r.Pos, existing.Pos)
			end := max(r.End(), existing.End())
			r = Range{Pos: start, Size: end - start}
		}
	}

	if !inserted {
		merged = append(merged, r)
	}

	rs.ranges = merged
}

// Present checks if a range is fully covered by existing ranges in this shard
func (rs *RangeShard) Present(r Range) bool {
	if r.Size <= 0 {
		return true
	}

	covered := int64(0)
	for _, existing := range rs.ranges {
		if existing.Overlaps(r) {
			intersection := existing.Intersect(r)
			covered += intersection.Size
		}
	}

	return covered >= r.Size
}

// GetRanges returns a copy of all ranges in this shard
func (rs *RangeShard) GetRanges() []Range {
	ranges := make([]Range, len(rs.ranges))
	copy(ranges, rs.ranges)
	return ranges
}

// Clear removes all ranges from the shard
func (rs *RangeShard) Clear() {
	rs.ranges = rs.ranges[:0]
}

// RangeSnapshot provides a read-only view of all ranges
type RangeSnapshot struct {
	version   int64
	ranges    []Range // Sorted and merged ranges
	createdAt time.Time
}

// GetRanges returns a copy of all ranges in the snapshot
func (rs *RangeSnapshot) GetRanges() []Range {
	if rs == nil {
		return []Range{}
	}
	result := make([]Range, len(rs.ranges))
	copy(result, rs.ranges)
	return result
}

// Present checks if a range is present in the snapshot
func (rs *RangeSnapshot) Present(r Range) bool {
	if r.Size <= 0 {
		return true
	}

	// Binary search for efficiency
	left, right := 0, len(rs.ranges)-1

	for left <= right {
		mid := (left + right) / 2
		midRange := rs.ranges[mid]

		if r.End() <= midRange.Pos {
			right = mid - 1
		} else if r.Pos >= midRange.End() {
			left = mid + 1
		} else {
			// Found overlap - check if fully contained
			if midRange.Contains(r) {
				return true
			}
			// Partial overlap - need to check adjacent ranges
			return rs.checkRangeCoverage(r, left, right)
		}
	}

	return false
}

// checkRangeCoverage checks if a range is fully covered by multiple adjacent ranges
func (rs *RangeSnapshot) checkRangeCoverage(r Range, startIdx, endIdx int) bool {
	covered := int64(0)

	// Expand search window to include adjacent ranges
	for i := max(0, startIdx-2); i < min(len(rs.ranges), endIdx+3); i++ {
		existing := rs.ranges[i]
		if existing.Overlaps(r) {
			intersection := existing.Intersect(r)
			covered += intersection.Size
		}
	}

	return covered >= r.Size
}

// ShardedRanges manages ranges across multiple shards with snapshot support
type ShardedRanges struct {
	shards [NumRangeShards]*RangeShard

	// Snapshot management
	snapshot      atomic.Pointer[RangeSnapshot]
	snapshotDirty atomic.Bool
	snapshotMu    sync.Mutex

	// Statistics
	stats struct {
		insertions     atomic.Int64
		lookups        atomic.Int64
		snapshotHits   atomic.Int64
		snapshotMisses atomic.Int64
	}
}

// NewShardedRanges creates a new sharded range manager
func NewShardedRanges() *ShardedRanges {
	sr := &ShardedRanges{}

	// Initialize all shards
	for i := range sr.shards {
		sr.shards[i] = &RangeShard{
			ranges: make([]Range, 0, 8), // Pre-allocate small capacity
		}
	}

	// Initialize empty snapshot
	sr.updateSnapshotLocked()

	return sr
}

// getShardsForRange returns all shards that might contain parts of the range
func (sr *ShardedRanges) getShardsForRange(r Range) []*RangeShard {
	startChunk := r.Pos / ShardChunkSize
	endChunk := (r.End() - 1) / ShardChunkSize

	startIdx := startChunk & ShardMask
	endIdx := endChunk & ShardMask

	var shards []*RangeShard

	if startIdx <= endIdx {
		// Normal case - no wraparound
		for i := startIdx; i <= endIdx; i++ {
			shards = append(shards, sr.shards[i])
		}
	} else {
		// Wraparound case (rare)
		for i := startIdx; i < NumRangeShards; i++ {
			shards = append(shards, sr.shards[i])
		}
		for i := int64(0); i <= endIdx; i++ {
			shards = append(shards, sr.shards[i])
		}
	}

	return shards
}

// Insert adds a range to ALL shards it spans
// This ensures Present() can find it regardless of which shard is checked
func (sr *ShardedRanges) Insert(r Range) {
	if r.Size <= 0 {
		return
	}

	sr.stats.insertions.Add(1)

	// Calculate which shards this range spans
	startChunk := r.Pos / ShardChunkSize
	endChunk := (r.End() - 1) / ShardChunkSize

	// Insert into all affected shards
	if startChunk == endChunk {
		// Single shard - fast path
		shard := sr.shards[startChunk&ShardMask]
		shard.mu.Lock()
		shard.Insert(r)
		shard.mu.Unlock()
	} else {
		// Multiple shards - insert into all
		shards := sr.getShardsForRange(r)
		for _, shard := range shards {
			shard.mu.Lock()
			shard.Insert(r)
			shard.mu.Unlock()
		}
	}

	// Mark snapshot as dirty
	sr.snapshotDirty.Store(true)
}

// InsertBatch efficiently inserts multiple ranges
func (sr *ShardedRanges) InsertBatch(ranges []Range) {
	if len(ranges) == 0 {
		return
	}

	sr.stats.insertions.Add(int64(len(ranges)))

	// Group ranges by ALL shards they span
	shardRanges := make(map[int][]Range)
	for _, r := range ranges {
		if r.Size <= 0 {
			continue
		}

		// Calculate all shards this range touches
		startChunk := r.Pos / ShardChunkSize
		endChunk := (r.End() - 1) / ShardChunkSize

		if startChunk == endChunk {
			// Single shard
			shardIndex := int(startChunk & ShardMask)
			shardRanges[shardIndex] = append(shardRanges[shardIndex], r)
		} else {
			// Multiple shards - add to all affected shards
			startIdx := int(startChunk & ShardMask)
			endIdx := int(endChunk & ShardMask)

			if startIdx <= endIdx {
				// No wraparound
				for i := startIdx; i <= endIdx; i++ {
					shardRanges[i] = append(shardRanges[i], r)
				}
			} else {
				// Wraparound case
				for i := startIdx; i < NumRangeShards; i++ {
					shardRanges[i] = append(shardRanges[i], r)
				}
				for i := 0; i <= endIdx; i++ {
					shardRanges[i] = append(shardRanges[i], r)
				}
			}
		}
	}

	// Insert into each shard (acquire locks in order to prevent deadlock)
	shardIndices := make([]int, 0, len(shardRanges))
	for idx := range shardRanges {
		shardIndices = append(shardIndices, idx)
	}
	sort.Ints(shardIndices)

	for _, shardIndex := range shardIndices {
		shard := sr.shards[shardIndex]
		shardRangeList := shardRanges[shardIndex]

		shard.mu.Lock()
		for _, r := range shardRangeList {
			shard.Insert(r)
		}
		shard.mu.Unlock()
	}

	sr.snapshotDirty.Store(true)
}

// Present checks if a range is fully present
func (sr *ShardedRanges) Present(r Range) bool {
	if r.Size <= 0 {
		return true
	}

	sr.stats.lookups.Add(1)

	// Try snapshot first (lockless fast path)
	if snapshot := sr.snapshot.Load(); snapshot != nil {
		if snapshot.Present(r) {
			sr.stats.snapshotHits.Add(1)
			return true
		}
		sr.stats.snapshotMisses.Add(1)
	}

	// Fallback to shard checking
	shards := sr.getShardsForRange(r)

	if len(shards) == 1 {
		// Single shard - simple case
		shard := shards[0]
		shard.mu.RLock()
		present := shard.Present(r)
		shard.mu.RUnlock()
		return present
	}

	// Multiple shards - need to check coverage across all
	return sr.checkMultiShardPresent(r, shards)
}

// checkMultiShardPresent checks if a range spanning multiple shards is present
func (sr *ShardedRanges) checkMultiShardPresent(r Range, shards []*RangeShard) bool {
	// Acquire read locks in order to prevent deadlock
	for _, shard := range shards {
		shard.mu.RLock()
	}
	defer func() {
		for _, shard := range shards {
			shard.mu.RUnlock()
		}
	}()

	covered := int64(0)
	for _, shard := range shards {
		for _, existing := range shard.ranges {
			if existing.Overlaps(r) {
				intersection := existing.Intersect(r)
				covered += intersection.Size
				if covered >= r.Size {
					return true // Early exit
				}
			}
		}
	}

	return covered >= r.Size
}

// updateSnapshot refreshes the read-optimized snapshot
func (sr *ShardedRanges) updateSnapshot() {
	if !sr.snapshotDirty.CompareAndSwap(true, false) {
		return // Not dirty
	}

	sr.snapshotMu.Lock()
	defer sr.snapshotMu.Unlock()
	sr.updateSnapshotLocked()
}

// updateSnapshotLocked updates snapshot while holding mutex
func (sr *ShardedRanges) updateSnapshotLocked() {
	// Collect all ranges from all shards
	var allRanges []Range

	for _, shard := range sr.shards {
		shard.mu.RLock()
		shardRanges := shard.GetRanges()
		allRanges = append(allRanges, shardRanges...)
		shard.mu.RUnlock()
	}

	// Sort and merge overlapping ranges
	mergedRanges := sr.mergeAndSortRanges(allRanges)

	// Create new snapshot
	snapshot := &RangeSnapshot{
		version:   time.Now().UnixNano(),
		ranges:    mergedRanges,
		createdAt: time.Now(),
	}

	sr.snapshot.Store(snapshot)
}

// mergeAndSortRanges sorts and merges overlapping ranges
func (sr *ShardedRanges) mergeAndSortRanges(ranges []Range) []Range {
	if len(ranges) <= 1 {
		return ranges
	}

	// Sort by position
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].Pos < ranges[j].Pos
	})

	// Merge overlapping ranges
	var merged []Range
	current := ranges[0]

	for i := 1; i < len(ranges); i++ {
		next := ranges[i]
		if current.End() >= next.Pos {
			// Merge overlapping/adjacent ranges
			end := max(current.End(), next.End())
			current.Size = end - current.Pos
		} else {
			merged = append(merged, current)
			current = next
		}
	}
	merged = append(merged, current)

	return merged
}

// GetStats returns performance statistics
func (sr *ShardedRanges) GetStats() map[string]int64 {
	snapshot := sr.snapshot.Load()
	var snapshotRanges int64
	var snapshotAge int64
	if snapshot != nil {
		snapshotRanges = int64(len(snapshot.ranges))
		snapshotAge = time.Since(snapshot.createdAt).Milliseconds()
	}

	return map[string]int64{
		"insertions":      sr.stats.insertions.Load(),
		"lookups":         sr.stats.lookups.Load(),
		"snapshot_hits":   sr.stats.snapshotHits.Load(),
		"snapshot_misses": sr.stats.snapshotMisses.Load(),
		"snapshot_ranges": snapshotRanges,
		"snapshot_age_ms": snapshotAge,
	}
}

// Clear removes all ranges from all shards
func (sr *ShardedRanges) Clear() {
	for _, shard := range sr.shards {
		shard.mu.Lock()
		shard.Clear()
		shard.mu.Unlock()
	}

	sr.snapshotDirty.Store(true)
	sr.updateSnapshot()
}

// GetAllShards returns all shards for persistence
func (sr *ShardedRanges) GetAllShards() []*RangeShard {
	return sr.shards[:]
}

// GetSnapshot returns the current snapshot
func (sr *ShardedRanges) GetSnapshot() *RangeSnapshot {
	return sr.snapshot.Load()
}

// ForceSnapshotUpdate forces an immediate snapshot update
func (sr *ShardedRanges) ForceSnapshotUpdate() {
	sr.snapshotDirty.Store(true)
	sr.updateSnapshot()
}

// StartSnapshotUpdater starts a background goroutine to update snapshots periodically
func (sr *ShardedRanges) StartSnapshotUpdater(stopCh <-chan struct{}) {
	ticker := time.NewTicker(SnapshotUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sr.updateSnapshot()
		case <-stopCh:
			return
		}
	}
}
