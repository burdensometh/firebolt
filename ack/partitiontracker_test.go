package ack

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartitionTrackerContiguousPrefix(t *testing.T) {
	var commits []CommitRequest
	var mu sync.Mutex
	pt := NewPartitionTracker(2, 0, 1, func(req CommitRequest) {
		mu.Lock()
		commits = append(commits, req)
		mu.Unlock()
	})

	// complete 0,1,2 in order: each extends the prefix and commits
	pt.MarkCompleted(0)
	pt.MarkCompleted(1)
	pt.MarkCompleted(2)

	mu.Lock()
	assert.Equal(t, []CommitRequest{
		{Partition: 2, NextOffset: 1, Generation: 1},
		{Partition: 2, NextOffset: 2, Generation: 1},
		{Partition: 2, NextOffset: 3, Generation: 1},
	}, commits)
	mu.Unlock()

	next, buffered := pt.NextOffset()
	assert.Equal(t, int64(3), next)
	assert.Equal(t, 0, buffered)
}

func TestPartitionTrackerOutOfOrderDoesNotSkipHole(t *testing.T) {
	var last CommitRequest
	pt := NewPartitionTracker(0, 0, 1, func(req CommitRequest) { last = req })

	// complete 0, then 2 and 3 while 1 is still in flight: commit must not pass the hole
	pt.MarkCompleted(0)
	assert.Equal(t, int64(1), last.NextOffset)

	pt.MarkCompleted(2)
	pt.MarkCompleted(3)

	next, buffered := pt.NextOffset()
	assert.Equal(t, int64(1), next, "commit position must not advance past the hole at offset 1")
	assert.Equal(t, 2, buffered)
	assert.Equal(t, []int64{2, 3}, pt.BufferedOffsets())
	assert.Equal(t, int64(1), last.NextOffset, "no new commit should have fired for out-of-order completion")

	// filling the hole releases everything up to the new contiguous prefix in one commit
	pt.MarkCompleted(1)
	next, buffered = pt.NextOffset()
	assert.Equal(t, int64(4), next)
	assert.Equal(t, 0, buffered)
	assert.Equal(t, int64(4), last.NextOffset)
	assert.Equal(t, int64(1), last.Generation)
}

func TestPartitionTrackerStartOffset(t *testing.T) {
	var last CommitRequest
	// assignment resumes at offset 10 (e.g. after a prior committed prefix)
	pt := NewPartitionTracker(1, 10, 1, func(req CommitRequest) { last = req })

	pt.MarkCompleted(10)
	assert.Equal(t, int64(11), last.NextOffset)

	// an offset below the start is already covered and must be ignored
	pt.MarkCompleted(5)
	assert.Equal(t, int64(11), last.NextOffset)
}

func TestPartitionTrackerFenceGeneration(t *testing.T) {
	var commitCount int
	pt := NewPartitionTracker(0, 0, 7, func(req CommitRequest) { commitCount++ })

	pt.MarkCompleted(0)
	before := commitCount

	pt.FenceGeneration()
	assert.True(t, pt.IsFenced())

	// late callbacks from the old assignment after rebalance must be discarded, never committed
	pt.MarkCompleted(1)
	pt.MarkCompleted(2)
	assert.Equal(t, before, commitCount, "fenced generation must not issue further commits")

	next, buffered := pt.NextOffset()
	assert.Equal(t, int64(1), next)
	assert.Equal(t, 0, buffered)
}
