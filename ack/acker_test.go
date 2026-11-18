package ack

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAckerDisabled(t *testing.T) {
	a := NewAcker(false, nil)
	assert.False(t, a.Enabled())

	a.AssignPartition(0, 0)
	a.RevokePartition(0)
	assert.Nil(t, a.NewRecordTracker(0, 0))
	a.RevokeAll() // must not panic
}

func TestAckerEndToEndCompletion(t *testing.T) {
	var mu sync.Mutex
	var commits []CommitRequest
	a := NewAcker(true, func(req CommitRequest) {
		mu.Lock()
		commits = append(commits, req)
		mu.Unlock()
	})

	gen := a.AssignPartition(3, 0)
	assert.Equal(t, int64(1), gen)

	tr0 := a.NewRecordTracker(3, 0)
	tr1 := a.NewRecordTracker(3, 1)
	tr2 := a.NewRecordTracker(3, 2)
	assert.NotNil(t, tr0)

	// open a branch on each so the records stay in flight while we complete them out of order
	tr0.EnterRoot()
	tr1.EnterRoot()
	tr2.EnterRoot()

	// record 1 finishes first: buffered behind record 0, no commit yet
	tr1.Terminal()
	mu.Lock()
	assert.Empty(t, commits)
	mu.Unlock()

	// record 0 finishes: prefix advances through 1 -> commit next offset 2
	tr0.Terminal()
	mu.Lock()
	assert.Equal(t, []CommitRequest{{Partition: 3, NextOffset: 2, Generation: 1}}, commits)
	mu.Unlock()

	// record 2 finishes: prefix extends to next offset 3
	tr2.Terminal()
	mu.Lock()
	assert.Equal(t, CommitRequest{Partition: 3, NextOffset: 3, Generation: 1}, commits[len(commits)-1])
	mu.Unlock()
}

func TestAckerRebalanceFencesStaleGeneration(t *testing.T) {
	var mu sync.Mutex
	var commits []CommitRequest
	a := NewAcker(true, func(req CommitRequest) {
		mu.Lock()
		commits = append(commits, req)
		mu.Unlock()
	})

	// generation 1: partition 0 assigned starting at offset 0
	a.AssignPartition(0, 0)
	oldTracker := a.NewRecordTracker(0, 0)
	oldTracker.EnterRoot()

	// rebalance: partition revoked then reassigned -> generation 2 starts at offset 0 again
	a.RevokePartition(0)
	gen2 := a.AssignPartition(0, 0)
	assert.Equal(t, int64(2), gen2)

	// a late completion callback from work that belonged to the old generation arrives
	oldTracker.Terminal()

	mu.Lock()
	assert.Empty(t, commits, "a stale-generation completion must never produce a commit")
	mu.Unlock()

	// a brand-new tracker under generation 2 works normally
	newTracker := a.NewRecordTracker(0, 0)
	assert.NotNil(t, newTracker)
	newTracker.EnterRoot()
	newTracker.Terminal()
	mu.Lock()
	assert.Equal(t, []CommitRequest{{Partition: 0, NextOffset: 1, Generation: 2}}, commits)
	mu.Unlock()
}

func TestAckerRevokeAllAndUnknownPartition(t *testing.T) {
	a := NewAcker(true, func(req CommitRequest) {})
	a.AssignPartition(0, 0)
	a.AssignPartition(1, 0)

	a.RevokeAll()

	// new trackers cannot be created against fenced partitions
	assert.Nil(t, a.NewRecordTracker(0, 0))
	assert.Nil(t, a.NewRecordTracker(1, 0))

	// unknown partition
	assert.Nil(t, a.NewRecordTracker(99, 0))
}

func TestAckerReassignmentIncrementsGeneration(t *testing.T) {
	commits := make(chan CommitRequest, 10)
	a := NewAcker(true, func(req CommitRequest) { commits <- req })

	g1 := a.AssignPartition(0, 0)
	a.RevokePartition(0)
	g2 := a.AssignPartition(0, 5)
	a.RevokePartition(0)
	g3 := a.AssignPartition(0, 5)

	assert.Equal(t, int64(1), g1)
	assert.Equal(t, int64(2), g2)
	assert.Equal(t, int64(3), g3)

	tr := a.NewRecordTracker(0, 5)
	tr.EnterRoot()
	tr.Terminal()
	req := <-commits
	assert.Equal(t, int64(3), req.Generation)
	assert.Equal(t, int64(6), req.NextOffset)
}
