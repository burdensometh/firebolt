package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/metrics"

	elastic "github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// isolatedBulkFactory hands out one per-test mock bulk service.  Using an
// isolated mock per test avoids races against the package-level
// bulkServiceMock, whose long-lived workers are still exercised by the node
// tests in elasticsearch_test.go.
type isolatedBulkFactory struct {
	svc *mockBulkService
}

func (f isolatedBulkFactory) BulkService() bulkService { return f.svc }

func newIsolatedBulkService() *mockBulkService {
	return &mockBulkService{}
}

// newTestClient starts client machinery backed by an isolated mock bulk
// service, which is returned so the test can configure expectations on it.
func newTestClient(t *testing.T, batchSize, workers int, maxBulkBytes, memoryLimit int64) (*ElasticIndexClient, *mockBulkService, context.CancelFunc) {
	t.Helper()
	svc := newIsolatedBulkService()
	c, cancel := newTestClientWithFactory(t, isolatedBulkFactory{svc: svc}, batchSize, workers, maxBulkBytes, memoryLimit)
	return c, svc, cancel
}

func newTestClientWithFactory(t *testing.T, factory bulkServiceFactory, batchSize, workers int, maxBulkBytes, memoryLimit int64) (*ElasticIndexClient, context.CancelFunc) {
	t.Helper()
	metrics.Init("elasticsearch_index_client_test")
	m := &Metrics{}
	m.RegisterElasticIndexMetrics(0.01, 40, 8)

	ctx, cancel := context.WithCancel(context.Background())
	c := NewElasticIndexClient(
		ctx,
		factory,
		m,
		batchSize,
		3, // maxRetries
		5, // timeoutSeconds
		workers,
		50*time.Millisecond,
		maxBulkBytes,
		memoryLimit,
	)
	go c.Run(ctx)
	// every test must wait for in-flight bulk calls to finish before it
	// returns, so no worker goroutine outlives the test touching its mock
	t.Cleanup(func() {
		cancel()
		c.waitForInFlight()
	})
	return c, cancel
}

func newTestRequest(doc interface{}, docID string) *eventIndexRequest {
	event := &firebolt.AsyncEvent{
		Event: &firebolt.Event{Payload: IndexRequest{Index: "default", Doc: doc}, Created: time.Now()},
		// no-op defaults so terminal paths never panic when a test forgets one
		ReturnError:    func(error) {},
		ReturnEvent:    func(*firebolt.AsyncEvent) {},
		ReturnFiltered: func() {},
	}
	return &eventIndexRequest{
		Index: "default",
		Doc:   doc,
		DocID: docID,
		Event: event,
	}
}

func expectBulkSuccess(svc *mockBulkService) {
	svc.On("Add", mock.Anything).Return(nil)
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).Return(&elastic.BulkResponse{}, nil)
}

func waitForAssertion(t *testing.T, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.True(t, condition(), msg)
}

func TestEncodedBulkIndexRequestSizeMatchesWireFormat(t *testing.T) {
	req := elastic.NewBulkIndexRequest().Index("myindex").Type("mytype").Id("id-42").Doc(map[string]interface{}{
		"msg":  "hello",
		"user": 7,
	})

	size, err := encodedBulkIndexRequestSize(req)
	assert.NoError(t, err)

	lines, err := req.Source()
	assert.NoError(t, err)
	var expected int64
	for _, line := range lines {
		expected += int64(len(line)) + 1 // each line plus its trailing newline
	}
	assert.Equal(t, expected, size)
	assert.Greater(t, size, int64(0))
}

func TestSendRejectsOversizedDocumentWithoutBlockingOthers(t *testing.T) {
	// the oversized document does not fit one bulk request; the small one does
	c, svc, cancel := newTestClient(t, 10, 1, 60, 1000)
	defer cancel()

	oversized := newTestRequest("i am a perfectly ordinary log line", "doc-1")

	err := c.Send(oversized)
	assert.Error(t, err)

	var fbErr firebolt.FBError
	assert.True(t, errors.As(err, &fbErr))
	assert.Equal(t, errorCodeDocTooLarge, fbErr.Code)
	assert.Equal(t, int64(0), c.mem.InUse(), "rejected document must not reserve any memory")
	// and nothing was handed to ES
	svc.AssertNotCalled(t, "Do", mock.Anything)

	// a document that fits still flows through normally and reaches ES
	expectBulkSuccess(svc)
	successDone := make(chan struct{})
	ok := newTestRequest("small", "doc-2")
	ok.Event.ReturnEvent = func(*firebolt.AsyncEvent) { close(successDone) }
	assert.NoError(t, c.Send(ok))

	select {
	case <-successDone:
	case <-time.After(2 * time.Second):
		t.Fatal("valid document after an oversized one was not indexed")
	}
	waitForAssertion(t, time.Second, func() bool { return c.mem.InUse() == 0 }, "memory budget not released after indexing")
}

func TestSendRejectsDocumentLargerThanMemoryBudget(t *testing.T) {
	c, _, cancel := newTestClient(t, 10, 1, 1000, 100)
	defer cancel()

	req := newTestRequest(strings.Repeat("x", 500), "huge")
	err := c.Send(req)
	assert.Error(t, err)
	var fbErr firebolt.FBError
	assert.True(t, errors.As(err, &fbErr))
	assert.Equal(t, errorCodeDocTooLarge, fbErr.Code)
	assert.Equal(t, int64(0), c.mem.InUse())
}

func TestSendRejectsUnmarshallableDocument(t *testing.T) {
	c, _, cancel := newTestClient(t, 10, 1, 10*1024*1024, 100*1024*1024)
	defer cancel()

	req := newTestRequest(make(chan int), "bad-doc") // channels cannot be JSON-marshalled
	err := c.Send(req)
	assert.Error(t, err)
	var fbErr firebolt.FBError
	assert.True(t, errors.As(err, &fbErr))
	assert.Equal(t, errorCodeDocMarshal, fbErr.Code)
	assert.Equal(t, int64(0), c.mem.InUse())
}

func TestFlushOnBatchSize(t *testing.T) {
	c, svc, cancel := newTestClient(t, 3, 1, 10*1024*1024, 10*1024*1024)
	defer cancel()

	var mu sync.Mutex
	doCalls := 0
	svc.On("Add", mock.Anything).Return(nil)
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).Run(func(mock.Arguments) {
		mu.Lock()
		doCalls++
		mu.Unlock()
	}).Return(&elastic.BulkResponse{}, nil)

	done := make(chan struct{}, 10)
	for i := 0; i < 6; i++ {
		req := newTestRequest(fmt.Sprintf("log-%d", i), fmt.Sprintf("id-%d", i))
		req.Event.ReturnEvent = func(*firebolt.AsyncEvent) { done <- struct{}{} }
		assert.NoError(t, c.Send(req))
	}
	// six documents at batch-size 3 => exactly two bulk requests
	waitForAssertion(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return doCalls == 2
	}, "expected two bulk requests for six documents at batch-size 3")

	for i := 0; i < 6; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("not all documents were acknowledged")
		}
	}
	waitForAssertion(t, time.Second, func() bool { return c.mem.InUse() == 0 }, "memory leaked after flush")
}

func TestFlushOnByteBudgetBeforeBatchSize(t *testing.T) {
	// bulk byte budget fits roughly two small documents, batch size is large so
	// only the byte budget can trigger flushes
	c, svc, cancel := newTestClient(t, 100, 1, 100, 10*1024*1024)
	defer cancel()

	var mu sync.Mutex
	doCalls := 0
	svc.On("Add", mock.Anything).Return(nil)
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).Run(func(mock.Arguments) {
		mu.Lock()
		doCalls++
		mu.Unlock()
	}).Return(&elastic.BulkResponse{}, nil)

	done := make(chan struct{}, 10)
	for i := 0; i < 5; i++ {
		req := newTestRequest(fmt.Sprintf("log-line-%02d", i), "")
		req.Event.ReturnEvent = func(*firebolt.AsyncEvent) { done <- struct{}{} }
		assert.NoError(t, c.Send(req))
	}
	for i := 0; i < 5; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("documents flushed on byte budget were not acknowledged")
		}
	}
	// five documents cannot fit into a single 100-byte bulk body, so the byte
	// budget must have produced more than one request
	waitForAssertion(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return doCalls > 1
	}, "byte budget did not trigger flushes before batch size")
	waitForAssertion(t, time.Second, func() bool { return c.mem.InUse() == 0 }, "memory leaked after byte-budget flush")
}

func TestFlushOnMaxWait(t *testing.T) {
	c, svc, cancel := newTestClient(t, 100, 1, 10*1024*1024, 10*1024*1024)
	defer cancel()
	expectBulkSuccess(svc)

	done := make(chan struct{})
	req := newTestRequest("lonely log line", "")
	req.Event.ReturnEvent = func(*firebolt.AsyncEvent) { close(done) }
	assert.NoError(t, c.Send(req))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("partial batch was not flushed after batchMaxWait")
	}
	waitForAssertion(t, time.Second, func() bool { return c.mem.InUse() == 0 }, "memory leaked after timer flush")
}

func TestBackpressureWhenMemoryBudgetExhausted(t *testing.T) {
	// block every bulk call so the budget fills up and Send must park
	proceed := make(chan struct{})
	var proceedOnce sync.Once
	releaseES := func() { proceedOnce.Do(func() { close(proceed) }) }

	c, svc, cancel := newTestClient(t, 1000, 1, 10*1024*1024, 1024)
	// registered after the client cleanup so it runs first (cleanups are LIFO):
	// unblock the mock before waiting for in-flight batches to terminate
	t.Cleanup(releaseES)
	defer cancel()

	svc.On("Add", mock.Anything).Return(nil)
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).Run(func(mock.Arguments) {
		<-proceed
	}).Return(&elastic.BulkResponse{}, nil)

	fillDone := make(chan struct{})
	go func() {
		defer close(fillDone)
		for i := 0; i < 200; i++ {
			req := newTestRequest(fmt.Sprintf("log-%04d", i), "")
			if err := c.Send(req); err != nil {
				return
			}
		}
	}()

	// the budget cannot reach its exact limit (the last admitted request must
	// leave room for itself), so assert on near-saturation
	waitForAssertion(t, 2*time.Second, func() bool {
		return c.mem.InUse() >= int64(900)
	}, "budget never filled")

	// with the budget full and ES blocked, a further Send must be parked rather
	// than returning; confirm by giving it time to (not) complete.
	parkedDone := make(chan struct{})
	go func() {
		_ = c.Send(newTestRequest("waiting document", ""))
		close(parkedDone)
	}()
	select {
	case <-parkedDone:
		t.Fatal("Send returned despite the memory budget being exhausted")
	case <-time.After(100 * time.Millisecond):
	}

	// unblock ES; the parked Send must then complete and everything drains
	releaseES()
	select {
	case <-parkedDone:
	case <-time.After(3 * time.Second):
		t.Fatal("parked Send did not complete after backpressure cleared")
	}
	select {
	case <-fillDone:
	case <-time.After(3 * time.Second):
		t.Fatal("producer did not finish after backpressure cleared")
	}
}

func TestPartialFailureRetriesOnlyFailedSubsetAndPreservesOrder(t *testing.T) {
	// first call: item 0 succeeds, item 1 fails with a retryable error
	failedItem := &elastic.BulkResponseItem{
		Status: 500,
		Error:  &elastic.ErrorDetails{Type: "es_rejected_execution_exception", Reason: "rejected"},
	}
	firstResponse := &elastic.BulkResponse{
		Errors: true,
		Items: []map[string]*elastic.BulkResponseItem{
			{"index": {Status: 201}},
			{"index": failedItem},
		},
	}
	// retry call: the failed subset succeeds
	secondResponse := &elastic.BulkResponse{}

	c, svc, cancel := newTestClient(t, 2, 1, 10*1024*1024, 10*1024*1024)
	defer cancel()

	svc.On("Add", mock.Anything).Return(nil)
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).
		Return(firstResponse, nil).Once()
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).
		Return(secondResponse, nil).Once()

	successes := make(chan string, 4)
	req0 := newTestRequest("doc-zero", "id-0")
	req0.Event.ReturnEvent = func(*firebolt.AsyncEvent) { successes <- "id-0" }
	req1 := newTestRequest("doc-one", "id-1")
	req1.Event.ReturnEvent = func(*firebolt.AsyncEvent) { successes <- "id-1" }

	assert.NoError(t, c.Send(req0))
	assert.NoError(t, c.Send(req1))

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case id := <-successes:
			got[id] = true
		case <-time.After(2 * time.Second):
			t.Fatal("not all documents eventually succeeded")
		}
	}
	assert.True(t, got["id-0"] && got["id-1"])

	// exactly two bulk calls: the original batch and the one-item retry subset
	svc.AssertNumberOfCalls(t, "Do", 2)
	waitForAssertion(t, time.Second, func() bool { return c.mem.InUse() == 0 }, "memory leaked after partial-failure retry")
}

func TestMappingFailureIsNotRetriedAndReturnsEventError(t *testing.T) {
	mappingFailure := &elastic.BulkResponseItem{
		Status: 400,
		Error:  &elastic.ErrorDetails{Type: "mapper_parsing_exception", Reason: "bad mapping"},
	}
	response := &elastic.BulkResponse{
		Errors: true,
		Items: []map[string]*elastic.BulkResponseItem{
			{"index": mappingFailure},
			{"index": {Status: 201}},
		},
	}
	c, svc, cancel := newTestClient(t, 2, 1, 10*1024*1024, 10*1024*1024)
	defer cancel()

	svc.On("Add", mock.Anything).Return(nil)
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).Return(response, nil)

	var indexErr error
	errDone := make(chan struct{})
	reqBad := newTestRequest("bad", "bad-id")
	reqBad.Event.ReturnError = func(err error) { indexErr = err; close(errDone) }
	okDone := make(chan struct{})
	reqGood := newTestRequest("good", "good-id")
	reqGood.Event.ReturnEvent = func(*firebolt.AsyncEvent) { close(okDone) }

	assert.NoError(t, c.Send(reqBad))
	assert.NoError(t, c.Send(reqGood))

	select {
	case <-errDone:
	case <-time.After(2 * time.Second):
		t.Fatal("mapping failure was not returned to the event")
	}
	var fbErr firebolt.FBError
	assert.True(t, errors.As(indexErr, &fbErr))
	assert.Equal(t, "ES_INDEX_ERROR", fbErr.Code)

	select {
	case <-okDone:
	case <-time.After(2 * time.Second):
		t.Fatal("sibling document was not indexed")
	}

	// the mapping failure must never trigger a retry
	time.Sleep(100 * time.Millisecond)
	svc.AssertNumberOfCalls(t, "Do", 1)
	waitForAssertion(t, time.Second, func() bool { return c.mem.InUse() == 0 }, "memory leaked after mapping failure")
}

func TestWholeBatchTransportFailureRetriesUntilSuccess(t *testing.T) {
	// the first call fails at the transport level, the retry succeeds
	c, svc, cancel := newTestClient(t, 1, 1, 10*1024*1024, 10*1024*1024)
	defer cancel()

	svc.On("Add", mock.Anything).Return(nil)
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).
		Return(nil, errors.New("connection refused")).Once()
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).
		Return(&elastic.BulkResponse{}, nil).Once()

	done := make(chan struct{})
	req := newTestRequest("resilient log", "id-1")
	req.Event.ReturnEvent = func(*firebolt.AsyncEvent) { close(done) }
	assert.NoError(t, c.Send(req))

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("batch was not retried to success after transport failures")
	}
	svc.AssertNumberOfCalls(t, "Do", 2)
	waitForAssertion(t, time.Second, func() bool { return c.mem.InUse() == 0 }, "memory leaked after retried batch")
}

func TestShutdownReleasesAllBufferedMemory(t *testing.T) {
	// block ES so requests pile up inside the client
	proceed := make(chan struct{})
	var proceedOnce sync.Once
	releaseES := func() { proceedOnce.Do(func() { close(proceed) }) }

	c, svc, cancel := newTestClient(t, 1000, 1, 10*1024*1024, 4096)
	t.Cleanup(releaseES) // runs before the client cleanup (LIFO)

	svc.On("Add", mock.Anything).Return(nil)
	svc.On("Do", mock.AnythingOfType("*context.timerCtx")).Run(func(mock.Arguments) {
		<-proceed
	}).Return(&elastic.BulkResponse{}, nil)

	go func() {
		for i := 0; i < 50; i++ {
			if err := c.Send(newTestRequest(fmt.Sprintf("log-%d", i), "")); err != nil {
				return
			}
		}
	}()
	waitForAssertion(t, 2*time.Second, func() bool { return c.mem.InUse() > 0 }, "no requests were buffered")

	// shutdown while requests are buffered and ES is blocked; unblocking ES and
	// the worker drain must release the whole budget
	cancel()
	releaseES()
	waitForAssertion(t, 5*time.Second, func() bool { return c.mem.InUse() == 0 }, "memory budget leaked across shutdown")
}

// sizeAssertingService computes the real encoded size of every batch from the
// BulkIndexRequest actions added to it, so tests can assert actual wire sizes.
type sizeAssertingService struct {
	limit       int64
	mu          sync.Mutex
	maxObserved int64
	batchCount  int
}

func (s *sizeAssertingService) Timeout(string) *elastic.BulkService { return nil }

func (s *sizeAssertingService) Add(reqs ...elastic.BulkableRequest) *elastic.BulkService {
	var total int64
	for _, req := range reqs {
		indexReq, ok := req.(*elastic.BulkIndexRequest)
		if !ok {
			continue
		}
		size, err := encodedBulkIndexRequestSize(indexReq)
		if err != nil {
			continue
		}
		total += size
	}
	s.mu.Lock()
	if total > s.maxObserved {
		s.maxObserved = total
	}
	s.batchCount++
	s.mu.Unlock()
	return nil
}

func (s *sizeAssertingService) NumberOfActions() int { return 0 }

func (s *sizeAssertingService) Do(ctx context.Context) (*elastic.BulkResponse, error) {
	return &elastic.BulkResponse{}, nil
}

type sizeAssertingFactory struct {
	service *sizeAssertingService
}

func (f sizeAssertingFactory) BulkService() bulkService { return f.service }

func TestNoBulkRequestExceedsByteLimit(t *testing.T) {
	// property-style check with a tight byte budget: every batch actually
	// handed to a BulkService must stay within the configured limit.
	const limit int64 = 300
	recorder := &sizeAssertingService{limit: limit}
	factory := sizeAssertingFactory{service: recorder}

	c, cancel := newTestClientWithFactory(t, factory, 1000, 2, limit, 10*1024*1024)
	defer cancel()

	done := make(chan struct{}, 40)
	for i := 0; i < 40; i++ {
		req := newTestRequest(fmt.Sprintf("log-line-number-%02d", i), "")
		req.Event.ReturnEvent = func(*firebolt.AsyncEvent) { done <- struct{}{} }
		assert.NoError(t, c.Send(req))
	}
	for i := 0; i < 40; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("not all documents were indexed in the byte-limit test")
		}
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	assert.Greater(t, recorder.batchCount, 1, "documents should have required multiple bulk requests")
	assert.LessOrEqual(t, recorder.maxObserved, limit, "a bulk request exceeded the configured byte limit")
}
