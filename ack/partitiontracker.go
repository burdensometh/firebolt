package ack

import (
	"sort"
	"sync"

	log "github.com/sirupsen/logrus"
)

// CommitRequest is the next offset that should be committed for a partition: Kafka offset semantics
// commit "the next offset to consume", so NextOffset equals (highest completed offset + 1).
type CommitRequest struct {
	Partition  int32
	NextOffset int64
	Generation int64
}

// CommitFunc performs the actual offset commit against the source system.  The generation is passed
// through so callers can fence stale commits after a rebalance; the PartitionTracker never calls
// CommitFunc for a superseded generation.
type CommitFunc func(req CommitRequest)

// PartitionTracker records which source records have been fully acknowledged within a single
// partition assignment, and computes the longest *contiguous completed prefix* that is safe to
// commit.
//
// Out-of-order completion can never advance the commit position past a hole: if records 0..4 and 6
// are complete but record 5 is still in flight, only offset 5 (next offset after 4) may be
// committed.  Record 6 stays buffered until record 5 completes.
//
// Each partition assignment gets its own generation.  When a partition is revoked (a rebalance),
// FenceGeneration() invalidates the current state: no further completion callbacks from in-flight
// work belonging to the old assignment can produce a commit, which prevents a late callback from
// committing offsets against a newer assignment owned by this or another consumer instance.
type PartitionTracker struct {
	partition  int32
	generation int64

	mu sync.Mutex

	// nextToCommit is the lowest offset not yet known to be committed-equivalent; records below it
	// form the contiguous completed prefix.
	nextToCommit int64

	// completed holds the acknowledged offsets at or above nextToCommit that are waiting for gaps
	// ahead of them to fill.
	completed map[int64]struct{}

	// fenced is true once the assignment owning the current generation has been revoked or shut
	// down; all further completion events are ignored.
	fenced bool

	commitFunc CommitFunc
}

// NewPartitionTracker creates a tracker for one partition assignment.  startOffset is the offset at
// which this assignment began consuming (the first in-flight record offset); generation uniquely
// identifies the assignment so stale callbacks can be fenced.
func NewPartitionTracker(partition int32, startOffset int64, generation int64, commitFunc CommitFunc) *PartitionTracker {
	return &PartitionTracker{
		partition:    partition,
		generation:   generation,
		nextToCommit: startOffset,
		completed:    make(map[int64]struct{}),
		commitFunc:   commitFunc,
	}
}

// Generation returns the assignment generation this tracker was created for.
func (p *PartitionTracker) Generation() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.generation
}

// MarkCompleted records that the record at the given offset is fully acknowledged.  If the record
// extends the contiguous completed prefix, the new prefix is committed immediately via CommitFunc.
// Completions arriving after FenceGeneration are discarded.
func (p *PartitionTracker) MarkCompleted(offset int64) {
	p.mu.Lock()
	if p.fenced {
		p.mu.Unlock()
		log.WithField("partition_id", p.partition).WithField("offset", offset).
			Debug("ack: ignoring completion for fenced partition generation")
		return
	}

	if offset < p.nextToCommit {
		// already covered by a previously committed contiguous prefix
		p.mu.Unlock()
		return
	}

	p.completed[offset] = struct{}{}

	// advance the contiguous prefix as far as the completed set allows
	advanced := false
	for {
		if _, ok := p.completed[p.nextToCommit]; !ok {
			break
		}
		delete(p.completed, p.nextToCommit)
		p.nextToCommit++
		advanced = true
	}

	// out-of-order completion behind a hole must never produce a commit; only an actual extension
	// of the contiguous completed prefix does
	if !advanced {
		p.mu.Unlock()
		return
	}

	req := CommitRequest{
		Partition:  p.partition,
		NextOffset: p.nextToCommit,
		Generation: p.generation,
	}
	fn := p.commitFunc
	p.mu.Unlock()

	if fn != nil {
		fn(req)
	}
}

// FenceGeneration invalidates this tracker so that late completion callbacks from the superseded
// assignment can no longer trigger commits.  Buffered, unacknowledged state is intentionally
// retained in memory only; it is not committed and will be replayed by the next assignment from the
// last committed Kafka offset.
func (p *PartitionTracker) FenceGeneration() {
	p.mu.Lock()
	p.fenced = true
	buffered := len(p.completed)
	next := p.nextToCommit
	p.mu.Unlock()

	log.WithField("partition_id", p.partition).WithField("generation", p.generation).
		WithField("next_offset", next).WithField("buffered_completed", buffered).
		Info("ack: partition generation fenced; unacknowledged records will be replayed from the last committed offset")
}

// IsFenced reports whether this assignment generation has been invalidated.
func (p *PartitionTracker) IsFenced() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fenced
}

// NextOffset returns the next offset safe to commit (one past the contiguous completed prefix) and
// the count of completed offsets currently held behind a gap.  Intended for tests and metrics.
func (p *PartitionTracker) NextOffset() (int64, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextToCommit, len(p.completed)
}

// BufferedOffsets returns a sorted snapshot of acknowledged offsets waiting behind gaps.  Intended
// for tests.
func (p *PartitionTracker) BufferedOffsets() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]int64, 0, len(p.completed))
	for off := range p.completed {
		out = append(out, off)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
