package elasticsearch

import (
	"context"
	"sync"
)

// memorySemaphore is a weighted, FIFO semaphore that bounds the total number
// of encoded request bytes that may be buffered at any one time.  Every
// request admitted to the index client must acquire its encoded size before it
// is queued, and only releases it once the request reaches a terminal state
// (indexed, returned as an error, or discarded on shutdown).
//
// Blocked callers park on per-waiter channels inside a FIFO wait queue rather
// than each spinning up additional machinery: an upstream that produces faster
// than Elasticsearch can drain therefore applies backpressure with a bounded
// number of parked goroutines instead of accumulating unbounded memory or
// goroutines.
//
// A request larger than the configured limit is rejected immediately and can
// never block requests behind it.
type memorySemaphore struct {
	mu    sync.Mutex
	limit int64
	inUse int64

	// waiters is a FIFO queue of blocked acquisitions.  Each entry is a channel
	// that is closed when budget has been reserved for that waiter.
	waiters []*semaphoreWaiter
}

type semaphoreWaiter struct {
	size  int64
	ready chan struct{}
}

// newMemorySemaphore constructs a semaphore allowing at most limit bytes to be
// acquired concurrently.
func newMemorySemaphore(limit int64) *memorySemaphore {
	return &memorySemaphore{limit: limit}
}

// parkedWaiters returns the number of Acquire callers currently blocked in the
// FIFO queue.  It is intended for tests that need to synchronize on waiters
// being parked rather than merely enqueued.
func (m *memorySemaphore) parkedWaiters() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, w := range m.waiters {
		select {
		case <-w.ready:
		default:
			n++
		}
	}
	return n
}

// Limit returns the total byte budget.
func (m *memorySemaphore) Limit() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.limit
}

// InUse returns the number of bytes currently acquired.
func (m *memorySemaphore) InUse() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inUse
}

// TryAcquire attempts to reserve size bytes without blocking.  It returns true
// on success.  A request larger than the semaphore limit can never be acquired.
// TryAcquire does not jump ahead of queued waiters.
func (m *memorySemaphore) TryAcquire(size int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if size > m.limit || len(m.waiters) > 0 || size > m.limit-m.inUse {
		return false
	}
	m.inUse += size
	return true
}

// Acquire reserves size bytes, blocking in FIFO order until enough of the
// budget is free or ctx is done.  A request larger than the configured limit
// returns ErrRequestExceedsMemoryBudget and never blocks; context cancellation
// while waiting returns the context's error and acquires nothing.
func (m *memorySemaphore) Acquire(ctx context.Context, size int64) error {
	if size > m.limit {
		return ErrRequestExceedsMemoryBudget
	}

	waiter := &semaphoreWaiter{size: size, ready: make(chan struct{})}
	m.mu.Lock()
	m.waiters = append(m.waiters, waiter)
	m.tryWakeWaitersLocked()
	m.mu.Unlock()

	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		m.cancelWaiter(waiter)
		return ctx.Err()
	}
} // Release returns size bytes to the budget and wakes queued waiters, in FIFO
// order, whose requests now fit.
func (m *memorySemaphore) Release(size int64) {
	m.mu.Lock()
	m.inUse -= size
	if m.inUse < 0 {
		m.inUse = 0
	}
	m.tryWakeWaitersLocked()
	m.mu.Unlock()
}

// tryWakeWaitersLocked reserves budget for every waiter at the head of the
// queue whose request currently fits.  A waiter whose request does not fit
// blocks everything behind it (strict FIFO), which makes wakeups deterministic.
// Callers must hold m.mu.
func (m *memorySemaphore) tryWakeWaitersLocked() {
	for len(m.waiters) > 0 {
		head := m.waiters[0]
		if head.size > m.limit-m.inUse {
			return
		}
		m.inUse += head.size
		m.waiters = m.waiters[1:]
		close(head.ready)
	}
}

// cancelWaiter removes a still-queued waiter (its context was canceled) and
// re-evaluates the queue.  If the waiter was already woken, its budget was
// reserved and nothing is changed.
func (m *memorySemaphore) cancelWaiter(waiter *semaphoreWaiter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, w := range m.waiters {
		if w == waiter {
			m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
			m.tryWakeWaitersLocked()
			return
		}
	}
}
