package executor

import (
	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/node"
)

// enterRootAck opens the single initial acknowledgement branch for a record emitted by a source
// that implements node.AckerSource.  Sources without manual acknowledgement are unaffected; events
// that already carry a tracker (replayed internal events) are left untouched.
func (e *Executor) enterRootAck(event *firebolt.Event) {
	if event.Acknowledger() != nil {
		return
	}

	ackerSource, ok := e.source.(node.AckerSource)
	if !ok {
		return
	}

	acker := ackerSource.Acker()
	if acker == nil || !acker.Enabled() {
		return
	}

	partition, offset, ok := sourceRecordPosition(event)
	if !ok {
		log.Debug("executor: event without source record position, cannot attach acknowledgement tracker")
		return
	}

	tracker := acker.NewRecordTracker(partition, offset)
	if tracker == nil {
		// partition already revoked/fenced; the record will be replayed by the next assignment
		return
	}
	tracker.EnterRoot()
	event.AttachAcknowledger(tracker)
}

// sourceRecordPosition extracts the source partition/offset carried by an event's payload.  Sources
// that do not expose a record position return false, in which case manual acknowledgement cannot
// bind the event.
func sourceRecordPosition(event *firebolt.Event) (int32, int64, bool) {
	position, ok := event.Payload.(firebolt.RecordPosition)
	if !ok {
		return 0, 0, false
	}
	return position.SourcePartition(), position.SourceOffset(), true
}
