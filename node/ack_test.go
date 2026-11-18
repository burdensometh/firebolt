package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.com/digitalocean/firebolt/metrics"
	"github.com/digitalocean/firebolt/node"
)

// countingAck is a test Acknowledger that records branch/terminal calls
type countingAck struct {
	branches  int
	terminals int
	completed chan struct{}
}

func (c *countingAck) Branch() { c.branches++ }
func (c *countingAck) Terminal() {
	c.terminals++
	if c.terminals == c.branches {
		select {
		case c.completed <- struct{}{}:
		default:
		}
	}
}

func newCountingAck() *countingAck {
	return &countingAck{completed: make(chan struct{}, 1)}
}

// waitForCompletion blocks until every opened branch has closed or the deadline passes.
func waitForCompletion(t *testing.T, a *countingAck, what string) {
	t.Helper()
	select {
	case <-a.completed:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s never reached all terminal branches: branches=%d terminals=%d", what, a.branches, a.terminals)
	}
	assert.Equal(t, a.branches, a.terminals)
}

// terminalSyncNode is a leaf sync node that always succeeds
type terminalSyncNode struct {
	fbcontext.ContextAware
}

func (terminalSyncNode) Setup(map[string]string) error { return nil }
func (terminalSyncNode) Shutdown() error               { return nil }
func (terminalSyncNode) Receive(fbcontext.Message) error {
	return errors.New("terminalSyncNode: message not supported")
}
func (terminalSyncNode) Process(e *firebolt.Event) (*firebolt.Event, error) {
	return e, nil
}

// failingNode always errors; used with and without an error handler
type failingNode struct {
	fbcontext.ContextAware
}

func (failingNode) Setup(map[string]string) error { return nil }
func (failingNode) Shutdown() error               { return nil }
func (failingNode) Receive(fbcontext.Message) error {
	return errors.New("failingNode: message not supported")
}
func (failingNode) Process(*firebolt.Event) (*firebolt.Event, error) {
	return nil, errors.New("boom")
}

// terminalErrorHandler is a sync error handler that always succeeds
type terminalErrorHandler struct {
	fbcontext.ContextAware
}

func (terminalErrorHandler) Setup(map[string]string) error { return nil }
func (terminalErrorHandler) Shutdown() error               { return nil }
func (terminalErrorHandler) Receive(fbcontext.Message) error {
	return errors.New("terminalErrorHandler: message not supported")
}
func (terminalErrorHandler) Process(e *firebolt.Event) (*firebolt.Event, error) {
	return e, nil
}

// filteringNode filters any event whose string payload starts with "filter"
type filteringNode struct {
	fbcontext.ContextAware
}

func (filteringNode) Setup(map[string]string) error { return nil }
func (filteringNode) Shutdown() error               { return nil }
func (filteringNode) Receive(fbcontext.Message) error {
	return errors.New("filteringNode: message not supported")
}
func (filteringNode) Process(e *firebolt.Event) (*firebolt.Event, error) {
	if s, ok := e.Payload.(string); ok && len(s) >= 6 && s[:6] == "filter" {
		return nil, nil
	}
	return e, nil
}

func registerAckTestNodes() {
	node.GetRegistry().RegisterNodeType("ackterminal", func() node.Node { return &terminalSyncNode{} }, nil, nil)
	node.GetRegistry().RegisterNodeType("ackfailing", func() node.Node { return &failingNode{} }, nil, nil)
	node.GetRegistry().RegisterNodeType("ackerrorhandler", func() node.Node { return &terminalErrorHandler{} }, nil, nil)
	node.GetRegistry().RegisterNodeType("ackfilter", func() node.Node { return &filteringNode{} }, nil, nil)
}

// startPump runs ProcessEvent on every event delivered to this node context until its channel is
// closed, mimicking the executor's worker loop for these hierarchy unit tests.
func startPump(nc *node.Context) {
	go func() {
		for ev := range nc.Ch {
			nc.ProcessEvent(&ev)
		}
	}()
}

// TestSyncNodeAckLeafAndFilter drives an event with an ack tracker through a success->leaf
// hierarchy and verifies terminal branch accounting.
func TestSyncNodeAckLeafAndFilter(t *testing.T) {
	metrics.Init("node_ack_test")
	registerAckTestNodes()

	cfg := &node.Config{
		ID:         "parent",
		Name:       "ackterminal",
		Workers:    1,
		BufferSize: 10,
		Children: []*node.Config{{
			ID:         "leaf",
			Name:       "ackterminal",
			Workers:    1,
			BufferSize: 10,
		}},
	}
	nc := node.InitNodeContextHierarchy(cfg)
	startPump(nc)
	startPump(nc.Children[0])

	ack1 := newCountingAck()
	event := &firebolt.Event{Payload: "x", Created: time.Now()}
	event.AttachAcknowledger(ack1)
	ack1.Branch()
	nc.ProcessEvent(event)

	waitForCompletion(t, ack1, "successful event")
}

// TestSyncNodeAckFiltered verifies a filtered event closes its single branch.
func TestSyncNodeAckFiltered(t *testing.T) {
	metrics.Init("node_ack_test")
	registerAckTestNodes()

	// self-contained filtering node with no children; filtering is then a terminal outcome
	cfg := &node.Config{
		ID:         "filter",
		Name:       "ackfilter",
		Workers:    1,
		BufferSize: 10,
	}
	nc := node.InitNodeContextHierarchy(cfg)

	ack2 := newCountingAck()
	filterEvent := &firebolt.Event{
		Payload: "filter this message",
		Created: time.Now(),
	}
	filterEvent.AttachAcknowledger(ack2)
	ack2.Branch()
	nc.ProcessEvent(filterEvent)

	waitForCompletion(t, ack2, "filtered event")
}

// TestAsyncNodeAckTerminal verifies an async node's deferred callback closes its branch.
func TestAsyncNodeAckTerminal(t *testing.T) {
	metrics.Init("node_ack_test")
	registerTestNodeTypes()

	// self-contained hierarchy with a terminal leaf so the async node's delayed callback cannot
	// leak events into the shared testconfig hierarchy used by other tests
	cfg := &node.Config{
		ID:         "async",
		Name:       "asyncfilternode",
		Workers:    1,
		BufferSize: 10,
		Children: []*node.Config{{
			ID:         "leaf",
			Name:       "ackterminal",
			Workers:    1,
			BufferSize: 10,
		}},
	}
	registerAckTestNodes()
	nc := node.InitNodeContextHierarchy(cfg)
	startPump(nc.Children[0])

	ack3 := newCountingAck()
	event := &firebolt.Event{
		Payload: "asyncfilter me",
		Created: time.Now(),
	}
	event.AttachAcknowledger(ack3)
	ack3.Branch()
	nc.ProcessEvent(event) // asyncfilternode filters after ~1s

	waitForCompletion(t, ack3, "async filtered event")
}

// TestErrorHandlerAckBranch verifies a node failure with an error handler opens a new branch that
// the error handler's own terminal outcome closes.
func TestErrorHandlerAckBranch(t *testing.T) {
	metrics.Init("node_ack_test")
	registerAckTestNodes()

	cfg := &node.Config{
		ID:         "failing",
		Name:       "ackfailing",
		Workers:    1,
		BufferSize: 10,
		ErrorHandler: &node.Config{
			ID:         "eh",
			Name:       "ackerrorhandler",
			Workers:    1,
			BufferSize: 10,
		},
	}
	nc := node.InitNodeContextHierarchy(cfg)
	startPump(nc.ErrorHandler)

	ack4 := newCountingAck()
	event := &firebolt.Event{Payload: "x", Created: time.Now()}
	event.AttachAcknowledger(ack4)
	ack4.Branch()
	nc.ProcessEvent(event)

	waitForCompletion(t, ack4, "error-handler branch")
	// the failing node's branch is transferred to the error handler, so no extra branch is opened
	assert.Equal(t, 1, ack4.branches)
	assert.Equal(t, 1, ack4.terminals)
}

// TestFailureWithoutErrorHandlerIsTerminal verifies a failure at a node without an error handler is
// itself a declared terminal outcome that closes the branch.
func TestFailureWithoutErrorHandlerIsTerminal(t *testing.T) {
	metrics.Init("node_ack_test")
	registerAckTestNodes()

	cfg := &node.Config{
		ID:         "failing",
		Name:       "ackfailing",
		Workers:    1,
		BufferSize: 10,
	}
	nc := node.InitNodeContextHierarchy(cfg)
	assert.Nil(t, nc.ErrorHandler)

	ack5 := newCountingAck()
	event := &firebolt.Event{Payload: "x", Created: time.Now()}
	event.AttachAcknowledger(ack5)
	ack5.Branch()
	nc.ProcessEvent(event)

	waitForCompletion(t, ack5, "failure without error handler")
	assert.Equal(t, 1, ack5.branches)
	assert.Equal(t, 1, ack5.terminals)
}

// TestFanoutAckBranches verifies a fanout node opens one branch per delivered child event and the
// child's leaf terminal outcomes eventually close them all.
func TestFanoutAckBranches(t *testing.T) {
	metrics.Init("node_ack_test")
	registerTestNodeTypes()
	registerAckTestNodes()

	// self-contained hierarchy: the built-in fanoutnode triples each event into an ackterminal
	// leaf, avoiding pollution of the shared testconfig hierarchies used by other tests
	cfg := &node.Config{
		ID:         "fanout",
		Name:       "fanoutnode",
		Workers:    1,
		BufferSize: 10,
		Children: []*node.Config{{
			ID:         "leaf",
			Name:       "ackterminal",
			Workers:    1,
			BufferSize: 10,
		}},
	}
	nc := node.InitNodeContextHierarchy(cfg)
	startPump(nc.Children[0])

	ack6 := newCountingAck()
	event := &firebolt.Event{Payload: "triple me", Created: time.Now()}
	event.AttachAcknowledger(ack6)
	ack6.Branch()
	nc.ProcessEvent(event)

	// fanout returns 3 events delivered to the single leaf: the first reuses the open branch,
	// each of the other two opens its own -> 3 total branches, each closed by the leaf
	waitForCompletion(t, ack6, "fanout branches")
	assert.Equal(t, 3, ack6.branches)
	assert.Equal(t, 3, ack6.terminals)
}

// TestDiscardOnFullBufferIsTerminal verifies a discarded delivery, including the first delivery
// that reuses the open branch, still closes the branch so the record cannot hang unacknowledged.
func TestDiscardOnFullBufferIsTerminal(t *testing.T) {
	metrics.Init("node_ack_test")
	registerAckTestNodes()

	cfg := &node.Config{
		ID:         "parent",
		Name:       "ackterminal",
		Workers:    1,
		BufferSize: 10,
		Children: []*node.Config{{
			ID:                  "sink",
			Name:                "ackterminal",
			Workers:             1,
			BufferSize:          0, // unbuffered and never drained -> every non-blocking send discards
			DiscardOnFullBuffer: true,
		}},
	}
	nc := node.InitNodeContextHierarchy(cfg)

	ack8 := newCountingAck()
	event := &firebolt.Event{Payload: "x", Created: time.Now()}
	event.AttachAcknowledger(ack8)
	ack8.Branch()
	nc.ProcessEvent(event)

	waitForCompletion(t, ack8, "discarded first delivery")
	// no extra branch was opened (single child, first delivery reuses the root branch)
	assert.Equal(t, 1, ack8.branches)
	assert.Equal(t, 1, ack8.terminals)
}

// TestAckPropagatedThroughWithPayload ensures WithPayload keeps the same ack handle.
func TestAckPropagatedThroughWithPayload(t *testing.T) {
	ack7 := newCountingAck()
	event := &firebolt.Event{Payload: "orig", Created: time.Now()}
	event.AttachAcknowledger(ack7)

	clone := event.WithPayload("new")
	assert.Same(t, ack7, clone.Acknowledger())

	ae := firebolt.NewAsyncEvent(event, func(error) {}, func(*firebolt.AsyncEvent) {}, func() {})
	aeClone := ae.WithPayload("newer")
	assert.Same(t, ack7, aeClone.Acknowledger())
}
