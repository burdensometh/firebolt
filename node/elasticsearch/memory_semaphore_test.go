package elasticsearch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemorySemaphoreTryAcquire(t *testing.T) {
	sem := newMemorySemaphore(100)

	assert.True(t, sem.TryAcquire(60))
	assert.Equal(t, int64(60), sem.InUse())

	// only 40 bytes remain
	assert.False(t, sem.TryAcquire(41))
	assert.Equal(t, int64(60), sem.InUse())

	assert.True(t, sem.TryAcquire(40))
	assert.Equal(t, int64(100), sem.InUse())

	// at the limit nothing more fits
	assert.False(t, sem.TryAcquire(1))

	// a request larger than the whole budget can never be acquired
	assert.False(t, sem.TryAcquire(101))
}

func TestMemorySemaphoreReleaseWakesWaiter(t *testing.T) {
	sem := newMemorySemaphore(100)
	assert.True(t, sem.TryAcquire(80))

	acquired := make(chan struct{})
	go func() {
		err := sem.Acquire(context.Background(), 40)
		assert.NoError(t, err)
		close(acquired)
	}()

	// waiter must still be parked: only 20 bytes are free
	select {
	case <-acquired:
		t.Fatal("acquire returned before enough budget was released")
	case <-time.After(50 * time.Millisecond):
	}

	sem.Release(80)

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("acquire was not woken by release")
	}
	assert.Equal(t, int64(40), sem.InUse())
}

func TestMemorySemaphoreAcquireLargerThanBudget(t *testing.T) {
	sem := newMemorySemaphore(100)
	err := sem.Acquire(context.Background(), 101)
	assert.ErrorIs(t, err, ErrRequestExceedsMemoryBudget)
	assert.Equal(t, int64(0), sem.InUse())
}

func TestMemorySemaphoreAcquireCanceledWhileWaiting(t *testing.T) {
	sem := newMemorySemaphore(100)
	assert.True(t, sem.TryAcquire(100))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- sem.Acquire(ctx, 10)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled acquire did not return")
	}

	// nothing was acquired and the budget is unchanged
	assert.Equal(t, int64(100), sem.InUse())

	// a later release must still work and the semaphore stays usable
	sem.Release(100)
	assert.True(t, sem.TryAcquire(50))
}

func TestMemorySemaphoreAcquireContextAlreadyDone(t *testing.T) {
	sem := newMemorySemaphore(10)
	assert.True(t, sem.TryAcquire(10))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, sem.Acquire(ctx, 1), context.Canceled)
}

func TestMemorySemaphoreNeverExceedsLimitUnderConcurrency(t *testing.T) {
	const (
		limit      = 1000
		goroutines = 64
		iterations = 200
	)
	sem := newMemorySemaphore(limit)

	var wg sync.WaitGroup
	var violations int32
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				size := int64(1 + (j % 50)) // 1..50 bytes
				if err := sem.Acquire(context.Background(), size); err != nil {
					t.Errorf("unexpected acquire error: %v", err)
					return
				}
				if sem.InUse() > limit {
					atomic.StoreInt32(&violations, 1)
				}
				sem.Release(size)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(0), violations)
	assert.Equal(t, int64(0), sem.InUse())
}

func TestMemorySemaphoreFIFOQueuesLargeWaiterBeforeSmallOne(t *testing.T) {
	sem := newMemorySemaphore(100)
	assert.True(t, sem.TryAcquire(95)) // 5 bytes remain; neither waiter fits

	// large waiter enters first (needs 80); wait until it is actually parked so
	// launch order cannot be reordered by the scheduler under -race
	largeDone := make(chan struct{})
	go func() {
		_ = sem.Acquire(context.Background(), 80)
		close(largeDone)
	}()
	waitForAssertion(t, time.Second, func() bool { return sem.parkedWaiters() == 1 }, "large waiter never parked")

	// small waiter enters second (needs 20); once the head waiter is served it
	// still will not fit, proving it cannot overtake
	smallDone := make(chan struct{})
	go func() {
		_ = sem.Acquire(context.Background(), 20)
		close(smallDone)
	}()
	waitForAssertion(t, time.Second, func() bool { return sem.parkedWaiters() == 2 }, "small waiter never parked")
	sem.mu.Lock()
	assert.Len(t, sem.waiters, 2)
	sem.mu.Unlock()

	// release 85: 90 bytes become free so the 80-byte head waiter fits
	// (inUse becomes 95-85+80 = 90); the 20-byte second waiter does not fit
	// behind it and must stay queued
	sem.Release(85)
	select {
	case <-largeDone:
	case <-time.After(time.Second):
		t.Fatal("head-of-queue waiter was not woken after release")
	}
	select {
	case <-smallDone:
		t.Fatal("queued waiter was woken out of FIFO order")
	case <-time.After(100 * time.Millisecond):
	}
	sem.mu.Lock()
	smallStillQueued := len(sem.waiters) == 1
	sem.mu.Unlock()
	assert.True(t, smallStillQueued, "queued waiter was woken out of FIFO order")

	// the head holder releasing frees room for the second waiter at last
	sem.Release(80)
	select {
	case <-smallDone:
	case <-time.After(time.Second):
		t.Fatal("second waiter was not woken after the head waiter released")
	}
}

func TestMemorySemaphoreTryAcquireAfterQueueDrains(t *testing.T) {
	sem := newMemorySemaphore(100)
	assert.True(t, sem.TryAcquire(100))

	done := make(chan struct{})
	go func() {
		_ = sem.Acquire(context.Background(), 50)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	assert.False(t, sem.TryAcquire(1), "TryAcquire must respect the FIFO queue")

	sem.Release(100)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queued waiter was not woken")
	}
	sem.Release(50)

	// with no waiters left, TryAcquire works again
	assert.True(t, sem.TryAcquire(25))
	assert.Equal(t, int64(25), sem.InUse())
}
