package ack

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrackerSingleBranch(t *testing.T) {
	var completed int32
	tr := NewTracker(7, func(record RecordID) {
		atomic.AddInt32(&completed, 1)
		assert.Equal(t, RecordID(7), record)
	})

	tr.EnterRoot()
	assert.Equal(t, int32(1), tr.Outstanding())
	assert.False(t, tr.IsComplete())

	tr.Terminal()
	assert.Equal(t, int32(1), atomic.LoadInt32(&completed))
	assert.True(t, tr.IsComplete())
	assert.Equal(t, int32(0), tr.Outstanding())

	// completion fires exactly once even with extra terminal calls
	tr.Terminal()
	assert.Equal(t, int32(1), atomic.LoadInt32(&completed))
}

func TestTrackerDagBranching(t *testing.T) {
	// simulate one root node fanning out to two children, one of which fans out again
	var completed int32
	tr := NewTracker(1, func(record RecordID) { atomic.AddInt32(&completed, 1) })

	tr.EnterRoot() // root branch

	// root delivers to two children: first delivery reuses root branch, second opens one
	tr.Branch()
	assert.Equal(t, int32(2), tr.Outstanding())

	// child A is a leaf and terminates
	tr.Terminal()
	assert.Equal(t, int32(0), atomic.LoadInt32(&completed))

	// child B fans out to two grandchildren
	tr.Branch()
	assert.Equal(t, int32(2), tr.Outstanding())

	tr.Terminal() // grandchild 1
	assert.False(t, tr.IsComplete())
	tr.Terminal() // grandchild 2 - last branch
	assert.True(t, tr.IsComplete())
	assert.Equal(t, int32(1), atomic.LoadInt32(&completed))
}

func TestTrackerCompleteFiresOnce(t *testing.T) {
	var completed int32
	tr := NewTracker(3, func(record RecordID) { atomic.AddInt32(&completed, 1) })
	tr.EnterRoot()
	tr.Branch()
	tr.Branch()

	tr.Complete()
	assert.True(t, tr.IsComplete())
	assert.Equal(t, int32(1), atomic.LoadInt32(&completed))

	tr.Complete()
	assert.Equal(t, int32(1), atomic.LoadInt32(&completed))
}

func TestTrackerConcurrentBranches(t *testing.T) {
	var completed int32
	tr := NewTracker(9, func(record RecordID) { atomic.AddInt32(&completed, 1) })
	tr.EnterRoot()

	const n = 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr.Branch()
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(n+1), tr.Outstanding())

	// close the n extra branches plus the root branch
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr.Terminal()
		}()
	}
	wg.Wait()
	assert.False(t, tr.IsComplete())
	tr.Terminal() // root
	assert.True(t, tr.IsComplete())
	assert.Equal(t, int32(1), atomic.LoadInt32(&completed))
}

func TestTrackerNilCallback(t *testing.T) {
	tr := NewTracker(0, nil)
	tr.EnterRoot()
	assert.NotPanics(t, func() { tr.Terminal() })
	assert.True(t, tr.IsComplete())
}
