// Package ack implements manual, at-least-once acknowledgement tracking for source records.
//
// A source record (e.g. a single Kafka message) flows through the whole node DAG, where it may be
// fanned out to multiple child branches, filtered, or handed to an error handler branch.  The record
// must only be acknowledged after *all* of its required terminal branches have reached a terminal
// state: a leaf node completed successfully, a node filtered the record, or a failure reached a
// declared terminal outcome (an error handler processed it, or the failing node declares no error
// handler and therefore the failure is terminal).
//
// This package deliberately provides no cross-system atomic transactions: an acknowledgement means
// "all in-process terminal branches are done", never "Kafka and every downstream datastore committed
// atomically".  On crash or shutdown, unacknowledged records are replayed from their last committed
// Kafka offset, which yields at-least-once delivery and requires downstream sinks to be idempotent.
package ack

import (
	"sync/atomic"
)

// RecordID identifies a single source record within a partition.  For Kafka this is the message
// offset; other sources may use their own monotonic sequence numbers.
type RecordID int64

// CompletionFunc is invoked exactly once, after the final outstanding branch of a record reaches a
// terminal state.  It must be safe to call from any goroutine; implementations must not block on
// node processing.
type CompletionFunc func(record RecordID)

// Tracker tracks the outstanding branches of a single source record as it traverses the node DAG.
// A record starts with a single outstanding branch (the root).  Each branch that reaches a node with
// children is replaced by one branch per child; a branch that reaches a terminal state decrements
// the outstanding count.  When the count reaches zero the record is complete and its CompletionFunc
// runs exactly once.
type Tracker struct {
	record      RecordID
	outstanding int32
	done        uint32 // atomically flipped to 1 once completion has fired
	onComplete  CompletionFunc
}

// NewTracker creates a Tracker for a single record.  EnterRoot must still be called once before the
// record enters the node hierarchy.
func NewTracker(record RecordID, onComplete CompletionFunc) *Tracker {
	return &Tracker{
		record:     record,
		onComplete: onComplete,
	}
}

// Record returns the source record id this tracker is responsible for.
func (t *Tracker) Record() RecordID {
	return t.record
}

// EnterRoot opens the single initial branch for a record as it enters the root node of the
// processing hierarchy.
func (t *Tracker) EnterRoot() {
	atomic.AddInt32(&t.outstanding, 1)
}

// Branch opens a new outstanding branch; used when one event is delivered to an additional child
// node or to an error handler.
func (t *Tracker) Branch() {
	atomic.AddInt32(&t.outstanding, 1)
}

// Terminal closes one outstanding branch.  When the last open branch closes, the record's
// CompletionFunc fires exactly once; later calls are no-ops.
func (t *Tracker) Terminal() {
	if atomic.AddInt32(&t.outstanding, -1) <= 0 {
		t.fireComplete()
	}
}

// Complete closes all outstanding branches at once.  It is used when a source cannot continue
// tracking a record (for example an internal tracking failure); prefer explicit Terminal calls for
// normal processing.  Like Terminal, it fires completion at most once.
func (t *Tracker) Complete() {
	atomic.StoreInt32(&t.outstanding, 0)
	t.fireComplete()
}

func (t *Tracker) fireComplete() {
	if atomic.CompareAndSwapUint32(&t.done, 0, 1) {
		if t.onComplete != nil {
			t.onComplete(t.record)
		}
	}
}

// Outstanding returns the number of branches still in flight for this record; intended for tests
// and metrics.
func (t *Tracker) Outstanding() int32 {
	return atomic.LoadInt32(&t.outstanding)
}

// IsComplete reports whether the record has already been acknowledged.
func (t *Tracker) IsComplete() bool {
	return atomic.LoadUint32(&t.done) == 1
}
