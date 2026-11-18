package ack

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

// Acker is the source-facing coordinator for manual acknowledgement.  A source creates one Acker,
// registers a PartitionTracker whenever a partition is assigned (fencing the previous generation
// for that partition on rebalance), and calls NewRecordTracker for every record it emits onto the
// processing pipeline.  When every terminal branch of a record completes, the record's Tracker fires
// its callback and the owning PartitionTracker advances the contiguous completed prefix, committing
// through the source-supplied CommitFunc.
type Acker struct {
	mu sync.Mutex

	partitions map[int32]*PartitionTracker
	enabled    bool

	// commitFunc is invoked with the next safe offset for a partition whenever its contiguous
	// completed prefix advances.
	commitFunc CommitFunc
}

// NewAcker creates an Acker.  When enabled is false the Acker is inert: NewRecordTracker returns
// nil and all other methods are safe no-ops, preserving the default auto-commit source behavior.
func NewAcker(enabled bool, commitFunc CommitFunc) *Acker {
	return &Acker{
		partitions: make(map[int32]*PartitionTracker),
		enabled:    enabled,
		commitFunc: commitFunc,
	}
}

// Enabled reports whether manual acknowledgement is turned on for this source.
func (a *Acker) Enabled() bool {
	return a.enabled
}

// AssignPartition registers a new assignment generation for a partition.  Any tracker from a prior
// generation of the same partition is fenced first, guaranteeing its late callbacks cannot commit
// against the new generation.  startOffset is the first offset the new assignment will consume.
// Returns the new assignment generation.
func (a *Acker) AssignPartition(partition int32, startOffset int64) int64 {
	if !a.enabled {
		return 0
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if prior, ok := a.partitions[partition]; ok {
		prior.FenceGeneration()
	}

	generation := int64(1)
	if prior, ok := a.partitions[partition]; ok {
		generation = prior.generation + 1
	}

	tracker := NewPartitionTracker(partition, startOffset, generation, a.commitFunc)
	a.partitions[partition] = tracker
	return generation
}

// RevokePartition fences the current assignment generation for a partition.  Completion callbacks
// from records belonging to the revoked generation become no-ops and no further commits are issued
// for it; unacknowledged records are left for replay from the last committed offset.
func (a *Acker) RevokePartition(partition int32) {
	if !a.enabled {
		return
	}

	a.mu.Lock()
	tracker, ok := a.partitions[partition]
	a.mu.Unlock()

	if ok {
		tracker.FenceGeneration()
	}
}

// RevokeAll fences every currently assigned partition; used on a full rebalance or shutdown.
func (a *Acker) RevokeAll() {
	if !a.enabled {
		return
	}

	a.mu.Lock()
	trackers := make([]*PartitionTracker, 0, len(a.partitions))
	for _, tracker := range a.partitions {
		trackers = append(trackers, tracker)
	}
	a.mu.Unlock()

	for _, tracker := range trackers {
		tracker.FenceGeneration()
	}
}

// NewRecordTracker creates a per-record completion tracker bound to the given partition.  The
// returned Tracker still needs EnterRoot called once when the record enters the root node.  Returns
// nil when manual acknowledgement is disabled or the partition is unknown/fenced.
func (a *Acker) NewRecordTracker(partition int32, offset int64) *Tracker {
	if !a.enabled {
		return nil
	}

	a.mu.Lock()
	tracker, ok := a.partitions[partition]
	a.mu.Unlock()

	if !ok || tracker.IsFenced() {
		return nil
	}

	generation := tracker.Generation()
	return NewTracker(RecordID(offset), func(record RecordID) {
		// re-resolve at completion time: a rebalance may have replaced the partition tracker.
		// PartitionTracker.MarkCompleted self-fences stale generations, and the closure captures
		// the generation the record was created under to double-check against a newer assignment.
		a.mu.Lock()
		current, ok := a.partitions[partition]
		a.mu.Unlock()
		if !ok || current.Generation() != generation {
			log.WithField("partition_id", partition).WithField("offset", offset).
				WithField("generation", generation).
				Warn("ack: discarding late record completion from a superseded partition generation; no commit issued")
			return
		}
		current.MarkCompleted(int64(record))
	})
}
