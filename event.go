package firebolt

import "time"

// Acknowledger is the per-record completion handle carried through the node DAG when a source runs
// in manual acknowledgement mode.  It is implemented by the framework's ack.Tracker; node code and
// application code never implement it.
//
// The contract is branch-based: a record enters the root node with one open branch; each delivery
// to an additional child or error handler opens a branch, and each terminal outcome closes one.
// The record is acknowledged to the source only after the final branch closes.
type Acknowledger interface {
	// Branch opens a new outstanding branch for the record.
	Branch()
	// Terminal closes one outstanding branch; the last close acknowledges the record.
	Terminal()
}

// RecordPosition is implemented by source payloads (or payload wrappers) that can identify the
// partition and sequence offset a record came from, which manual acknowledgement needs to bind an
// event to its source position.  The built-in kafkaconsumer attaches this on every record.
type RecordPosition interface {
	SourcePartition() int32
	SourceOffset() int64
}

// Event is the struct passed through the node graph.
type Event struct {
	Payload  interface{} `json:"payload"`
	Created  time.Time   `json:"created"`
	Recovery bool        `json:"recovery"`

	// ack is the manual-acknowledgement tracker for the source record this event descends from;
	// nil when the source uses automatic offsets or for internally generated events.  Unexported so
	// application payloads cannot tamper with acknowledgement state.
	ack Acknowledger
}

// AsyncEvent is a version of Event for nodes that support asynchronous processing.
type AsyncEvent struct {
	*Event
	ReturnError    func(error)
	ReturnEvent    func(*AsyncEvent)
	ReturnFiltered func()
}

// NewAsyncEvent creates a version of the passed Event suitable for asynchronous processing.
func NewAsyncEvent(event *Event, errFunc func(error), eventFunc func(*AsyncEvent), filterFunc func()) *AsyncEvent {
	return &AsyncEvent{
		Event:          event,
		ReturnError:    errFunc,
		ReturnEvent:    eventFunc,
		ReturnFiltered: filterFunc,
	}
}

// WithPayload returns a clone of this event with the payload replaced.  The acknowledgement handle
// is preserved so that downstream terminal outcomes still count against the same source record.
func (e *Event) WithPayload(payload interface{}) *Event {
	return &Event{
		Payload:  payload,
		Created:  e.Created,
		Recovery: e.Recovery,
		ack:      e.ack,
	}
}

// WithPayload returns a clone of this event with the payload replaced.
func (ae *AsyncEvent) WithPayload(payload interface{}) *AsyncEvent {
	return &AsyncEvent{
		Event: &Event{
			Payload:  payload,
			Created:  ae.Created,
			Recovery: ae.Recovery,
			ack:      ae.ack,
		},
	}
}

// AttachAcknowledger binds a completion tracker to this event.  It is called by sources that run in
// manual acknowledgement mode; passing nil clears any existing binding.
func (e *Event) AttachAcknowledger(a Acknowledger) {
	e.ack = a
}

// Acknowledger returns the tracker bound to this event, or nil when manual acknowledgement is not
// in use.
func (e *Event) Acknowledger() Acknowledger {
	return e.ack
}

// NewErrorHandlerEvent builds the channel event handed to an error handler, preserving the source
// record's acknowledgement handle so the error-handler branch is tracked to its own terminal
// outcome.
func (e *Event) NewErrorHandlerEvent(eventError EventError) Event {
	return Event{
		Payload:  eventError,
		Created:  time.Now(),
		Recovery: e.Recovery,
		ack:      e.ack,
	}
}
