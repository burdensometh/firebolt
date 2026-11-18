package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/digitalocean/firebolt"
	elastic "github.com/olivere/elastic/v7"

	log "github.com/sirupsen/logrus"
)

const (
	// errorCodeDocTooLarge is returned on the event when a single document's
	// encoded bulk representation is larger than the bulk request byte limit
	// (or the total buffered-memory budget).  Such a document is rejected on
	// its own and never blocks the documents that follow it.
	errorCodeDocTooLarge = "ES_DOC_TOO_LARGE"

	// errorCodeDocMarshal is returned on the event when a document cannot be
	// JSON-marshalled into a bulk request body.
	errorCodeDocMarshal = "ES_DOC_MARSHAL_ERROR"
)

var (
	// ErrMaxRetries is retained for compatibility; retry exhaustion is now
	// handled inside processBatch but callers may still match this sentinel.
	ErrMaxRetries = errors.New("max partial bulk retries reached")

	// ErrRequestExceedsMemoryBudget is returned by the memory semaphore when a
	// single request is larger than the entire buffered-memory budget.
	ErrRequestExceedsMemoryBudget = errors.New("request exceeds the configured buffered-memory budget")
)

type eventIndexRequest struct {
	Index       string
	MappingType string
	Doc         interface{}
	DocID       string
	Event       *firebolt.AsyncEvent

	// bulkRequest is the fully built ES bulk action.  It is constructed once at
	// admission time so that JSON marshalling and size accounting happen once.
	bulkRequest *elastic.BulkIndexRequest

	// encodedSize is the exact number of bytes this request contributes to a
	// bulk request body: the action/metadata line, the document source line,
	// and the trailing newline after each.
	encodedSize int64
}

// ElasticIndexClient is an implementation of the IndexClient interface.
type ElasticIndexClient struct {
	batchSize           int
	batchMaxWait        time.Duration
	maxRetries          int
	timeoutSeconds      int
	maxBulkRequestBytes int64
	workerPool          int

	// indexChan carries admitted requests from Send to the single batcher
	// goroutine.  It is unbuffered: when the batcher is busy, Send blocks and
	// backpressure flows upstream.
	indexChan chan *eventIndexRequest

	// batchChan hands completed batches to the fixed pool of worker goroutines.
	// It is bounded by the worker count, so batches waiting for a worker are
	// bounded too; their byte cost is already reserved against mem.
	batchChan chan []*eventIndexRequest

	// mem bounds the total encoded bytes of everything buffered anywhere in
	// this client: requests waiting to be batched, queued batches, in-flight
	// requests, and requests being retried.
	mem *memorySemaphore

	// inFlight tracks every batch currently being processed (including retries)
	// so callers can wait for all outstanding bulk calls to finish.
	inFlight sync.WaitGroup

	// idleWorkers tracks worker availability for the AvailableBatchRoutines gauge.
	idleWorkers int64

	connectionFactory bulkServiceFactory
	metrics           *Metrics

	ctx context.Context
}

// NewElasticIndexClient returns an IndexClient with a downstream elasticsearch connection.
func NewElasticIndexClient(
	ctx context.Context,
	connectionFactory bulkServiceFactory,
	metrics *Metrics,
	batchSize,
	maxRetries,
	timeoutSeconds,
	workerPool int,
	batchMaxWait time.Duration,
	maxBulkRequestBytes int64,
	memoryLimitBytes int64) *ElasticIndexClient {

	c := &ElasticIndexClient{
		connectionFactory:   connectionFactory,
		metrics:             metrics,
		batchSize:           batchSize,
		batchMaxWait:        batchMaxWait,
		maxRetries:          maxRetries,
		timeoutSeconds:      timeoutSeconds,
		maxBulkRequestBytes: maxBulkRequestBytes,
		workerPool:          workerPool,
		indexChan:           make(chan *eventIndexRequest),
		batchChan:           make(chan []*eventIndexRequest, workerPool),
		mem:                 newMemorySemaphore(memoryLimitBytes),
		idleWorkers:         int64(workerPool),
		ctx:                 ctx,
	}

	metrics.AvailableBatchRoutines.Set(float64(workerPool))
	metrics.MemoryBudgetBytes.Set(float64(memoryLimitBytes))
	return c
}

// Send prepares a document to be sent to elasticsearch.  It returns an error
// (without queueing anything) when the document cannot be marshalled, is itself
// larger than the allowed request size, or the client is shutting down.
// Otherwise it blocks while reserving the document's encoded size against the
// total buffered-memory budget, which is how backpressure is applied upstream.
func (c *ElasticIndexClient) Send(request *eventIndexRequest) error {
	// build the bulk action now: an unmarshallable document can never be
	// indexed and is rejected immediately with a locatable error instead of
	// being queued
	bulkRequest := elastic.NewBulkIndexRequest().Index(request.Index).Type(request.MappingType).Doc(request.Doc)
	if request.DocID != "" {
		bulkRequest = bulkRequest.Id(request.DocID)
	}

	size, err := encodedBulkIndexRequestSize(bulkRequest)
	if err != nil {
		c.metrics.IndexErrors.WithLabelValues("json_marshal_error").Inc()
		return firebolt.NewFBError(
			errorCodeDocMarshal,
			fmt.Sprintf("elasticsearch: failed to encode document for bulk indexing: %s", err.Error()),
			firebolt.WithInfo(map[string]interface{}{
				"index":  request.Index,
				"doc_id": request.DocID,
			}),
		)
	}
	request.bulkRequest = bulkRequest
	request.encodedSize = size

	// a single document larger than one whole bulk request can never be sent;
	// reject just this document so that the valid documents after it proceed
	if size > c.maxBulkRequestBytes {
		c.metrics.OversizedDocuments.Inc()
		return newOversizedDocumentError(request, c.maxBulkRequestBytes)
	}

	// reserve the buffered-memory budget before the request joins any queue.
	// Blocking here parks the caller on a condition variable rather than
	// spawning a waiting goroutine per request, so memory stays bounded.
	if err := c.acquire(c.ctx, size); err != nil {
		if errors.Is(err, ErrRequestExceedsMemoryBudget) {
			c.metrics.OversizedDocuments.Inc()
			return newOversizedDocumentError(request, c.mem.Limit())
		}
		return fmt.Errorf("elasticsearch: enqueue canceled while awaiting memory budget: %w", err)
	}

	select {
	case c.indexChan <- request:
		return nil
	case <-c.ctx.Done():
		c.release(size)
		return fmt.Errorf("elasticsearch: enqueue canceled: %w", c.ctx.Err())
	}
}

// Run runs the elasticsearch indexing client
func (c *ElasticIndexClient) Run(ctx context.Context) {
	for i := 0; i < c.workerPool; i++ {
		go c.worker(ctx)
	}
	go c.batch(ctx)

	<-ctx.Done()
	c.Stop()
}

// Stop stops the elasticsearch indexing client
func (c *ElasticIndexClient) Stop() {
	// allow some time to finish indexing all currently queued events
	time.Sleep(3 * time.Second)
}

// waitForInFlight blocks until every batch handed to a worker (including its
// retries) has reached a terminal state.  It is used in tests so that no worker
// goroutine is still touching the shared bulk-service mock when the next test
// resets it.
func (c *ElasticIndexClient) waitForInFlight() {
	c.inFlight.Wait()
}

// batch is the single goroutine that groups admitted requests into batches.  A
// batch is flushed when the first of these is reached: batchSize documents, the
// bulk request byte budget, or batchMaxWait of wall-clock time.
func (c *ElasticIndexClient) batch(ctx context.Context) {
	messages := make([]*eventIndexRequest, 0, c.batchSize)
	var batchBytes int64

	flush := func() {
		if len(messages) == 0 {
			return
		}
		select {
		case c.batchChan <- messages:
		case <-ctx.Done():
			// the workers are stopping and will never take this batch; give the
			// reserved memory back so the budget does not leak on shutdown
			for _, msg := range messages {
				c.release(msg.encodedSize)
			}
		}
		messages = make([]*eventIndexRequest, 0, c.batchSize)
		batchBytes = 0
	}

	timer := time.NewTimer(c.batchMaxWait)
	defer timer.Stop()

	for {
		select {
		case msg := <-c.indexChan:
			messages = append(messages, msg)
			batchBytes += msg.encodedSize
			if len(messages) >= c.batchSize || batchBytes >= c.maxBulkRequestBytes {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(c.batchMaxWait)
			}
		case <-timer.C:
			flush()
			timer.Reset(c.batchMaxWait)
		case <-ctx.Done():
			log.Info("shutting down index client batch goroutine")
			// release the budget for messages the batcher never got to flush
			for _, msg := range messages {
				c.release(msg.encodedSize)
			}
			return
		}
	}
}

// worker runs the actual bulk requests against Elasticsearch.  The number of
// workers is fixed, so the number of goroutines involved in indexing is bounded
// regardless of how many requests are waiting.
func (c *ElasticIndexClient) worker(ctx context.Context) {
	for {
		select {
		case batch := <-c.batchChan:
			c.runBatch(ctx, batch, true)
		case <-ctx.Done():
			// finish batches already queued, then exit.  These final attempts
			// are not retried: a failed final attempt frees its memory budget
			// and terminates so shutdown cannot hang forever against an
			// unavailable Elasticsearch.
			for {
				select {
				case batch := <-c.batchChan:
					c.runBatch(context.Background(), batch, false)
				default:
					return
				}
			}
		}
	}
}

// runBatch accounts a batch against inFlight and the idle-worker gauge and
// delegates to processBatch.  When retryOnFailure is false, transport-level
// failures terminate the batch immediately instead of being retried.
func (c *ElasticIndexClient) runBatch(ctx context.Context, batch []*eventIndexRequest, retryOnFailure bool) {
	c.inFlight.Add(1)
	defer c.inFlight.Done()
	c.metrics.AvailableBatchRoutines.Set(float64(atomic.AddInt64(&c.idleWorkers, -1)))
	c.processBatch(ctx, batch, retryOnFailure)
	c.metrics.AvailableBatchRoutines.Set(float64(atomic.AddInt64(&c.idleWorkers, 1)))
}

// processBatch sends one batch to Elasticsearch.  Transport-level failures
// retry the whole batch with exponential backoff; partial failures retry only
// the failed subset, preserving the original order of events.  The batch's
// memory budget remains reserved until each request reaches a terminal state,
// so retries consume no additional budget.  When retryOnFailure is false (the
// final drain during shutdown), a transport-level failure terminates the batch
// immediately instead of looping forever against an unreachable Elasticsearch.
func (c *ElasticIndexClient) processBatch(ctx context.Context, requests []*eventIndexRequest, retryOnFailure bool) {
	if len(requests) == 0 {
		return
	}

	// invariant guard: admission rejects anything larger than the byte budget
	// and the batcher flushes at the budget, so no real bulk request exceeds it
	if total := totalEncodedSize(requests); total > c.maxBulkRequestBytes {
		log.WithFields(log.Fields{
			"batch_bytes": total,
			"byte_limit":  c.maxBulkRequestBytes,
			"batch_size":  len(requests),
		}).Error("elasticsearch: batch exceeds the configured bulk request byte limit")
	}

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			// shutting down: stop retrying and free everything still outstanding
			c.releaseAll(requests)
			return
		}

		res, err := c.doBulkIndex(requests)
		if err == nil {
			if res != nil && res.Errors {
				// partial failure: only the failed subset may be retried
				retryRequests := c.handleErrorResponses(attempt, res, requests)
				if attempt >= c.maxRetries {
					// handleErrorResponses already returned terminal errors and
					// released the budget for every failed request
					return
				}
				if len(retryRequests) == 0 {
					return
				}
				requests = retryRequests
				continue
			}

			// request-level success with no individual errors
			for _, req := range requests {
				req.Event.ReturnEvent(req.Event)
			}
			c.releaseAll(requests)
			return
		}

		// whole-batch (transport-level) failure; typically means ES is unavailable
		c.metrics.BulkErrors.WithLabelValues(strconv.Itoa(attempt)).Inc()
		log.WithError(err).Error("elasticsearch: bulk index request failed")

		// while draining on shutdown, do not retry forever: report the failure
		// on each outstanding event and release its budget
		if !retryOnFailure {
			for _, req := range requests {
				req.Event.ReturnError(firebolt.NewFBError(
					"ES_INDEX_ERROR",
					"elasticsearch: bulk index failed during shutdown drain and was not retried",
					firebolt.WithInfo(err.Error()),
				))
			}
			c.releaseAll(requests)
			return
		}

		if !c.sleepWithBackoff(ctx, attempt) {
			c.releaseAll(requests)
			return
		}
	}
}

func (c *ElasticIndexClient) doBulkIndex(requests []*eventIndexRequest) (*elastic.BulkResponse, error) {
	// build the bulk request from the pre-built, pre-sized actions
	bulk := c.connectionFactory.BulkService()
	for _, req := range requests {
		bulk.Add(req.bulkRequest)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.timeoutSeconds)*time.Second)
	defer cancel()
	startBulk := time.Now()
	defer func() { c.metrics.BulkProcessTime.Observe(time.Since(startBulk).Seconds()) }()

	// send the bulk index request to elasticsearch
	res, err := bulk.Do(ctx)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		c.metrics.BulkTimeouts.Inc()
		return nil, ctx.Err()
	}

	return res, nil
}

// handleErrorResponses resolves the individual per-document outcomes of a bulk
// response.  Successful documents and non-retryable (mapping) failures reach a
// terminal state immediately and release their memory budget; retryable
// failures are returned as a subset, in their original batch order, with the
// budget retained.
func (c *ElasticIndexClient) handleErrorResponses(retryCount int, res *elastic.BulkResponse, requests []*eventIndexRequest) []*eventIndexRequest {
	c.metrics.BulkIndividualErrors.WithLabelValues(strconv.Itoa(retryCount)).Add(float64(len(res.Failed())))
	retryRequests := make([]*eventIndexRequest, 0)

	// res.Items is a []map[string]*BulkResponseItem, positionally aligned with
	// the requests that were sent. The map keys are "index", "update",
	// "delete", and "create"; we should only have "index" here.
	for bulkIndexPos, item := range res.Items {
		for action, i := range item {
			req := requests[bulkIndexPos]

			if action != "index" {
				// This should never happen based on our use of the es client
				log.WithField("action", action).Error("unexpected action returned from bulk index operation")
				continue
			}

			// Any 2xx response code should be considered a successful indexing operation
			if i.Status >= 200 && i.Status <= 299 {
				// success (this is the case where there were a mix of errors and successes in this bulk request)
				req.Event.ReturnEvent(req.Event)
				c.release(req.encodedSize)
				continue
			}

			if i.Error == nil {
				// nothing actionable can be retried or returned to the caller
				log.WithField("status_code", i.Status).Warn("non 2xx response code the bulk indexing operation, but the returned error field is nil")
				c.release(req.encodedSize)
				continue
			}

			log.WithFields(log.Fields{
				"es_error_type":   i.Error.Type,
				"es_error_reason": i.Error.Reason,
			}).Debug("bulk indexing failure")

			// don't retry mapping failures, zero chance of success on retry
			isTypeConflict := i.Error.Type == "mapper_parsing_exception"
			log.WithFields(log.Fields{
				"es_error_type":   i.Error.Type,
				"es_error_reason": i.Error.Reason,
				"type_conflict":   isTypeConflict,
			}).Debug("elasticsearch: failed to index, elasticsearch returned an error")

			if !isTypeConflict {
				retryRequests = append(retryRequests, req)
			}

			// return an error for operations that won't be retried
			if isTypeConflict || retryCount == c.maxRetries {
				e := firebolt.NewFBError("ES_INDEX_ERROR", "failed to index to elasticsearch", firebolt.WithInfo(i.Error))
				c.metrics.IndexErrors.WithLabelValues(i.Error.Type).Inc()
				req.Event.ReturnError(e)
				c.release(req.encodedSize)
			}

			// log errors at max retries
			if retryCount == c.maxRetries {
				log.WithFields(log.Fields{
					"es_error_type":   i.Error.Type,
					"es_error_reason": i.Error.Reason,
				}).Warn("elasticsearch: batch index operation failed after max retries")
			}
		}
	}

	if retryCount == c.maxRetries {
		c.metrics.BulkMaxRetriesReached.Add(float64(len(res.Failed())))
	}
	return retryRequests
}

// sleepWithBackoff sleeps with exponential backoff starting at 5s, then 10s,
// 30s, etc., capped at 3 minutes.  It returns false if ctx is canceled first.
func (c *ElasticIndexClient) sleepWithBackoff(ctx context.Context, attempt int) bool {
	maxSleep := 3 * time.Minute
	calcSleep := time.Duration(math.Pow(2, float64(attempt))) * 5 * time.Second
	sleep := time.Duration(math.Min(float64(maxSleep), float64(calcSleep)))

	timer := time.NewTimer(sleep)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *ElasticIndexClient) acquire(ctx context.Context, size int64) error {
	if err := c.mem.Acquire(ctx, size); err != nil {
		return err
	}
	c.metrics.BufferedBytes.Set(float64(c.mem.InUse()))
	return nil
}

func (c *ElasticIndexClient) release(size int64) {
	c.mem.Release(size)
	c.metrics.BufferedBytes.Set(float64(c.mem.InUse()))
}

func (c *ElasticIndexClient) releaseAll(requests []*eventIndexRequest) {
	for _, req := range requests {
		c.release(req.encodedSize)
	}
}

// encodedBulkIndexRequestSize returns the exact number of bytes the given bulk
// index request contributes to an Elasticsearch bulk request body.  This
// mirrors BulkService.bodyAsString in the olivere client, which writes each
// Source() line followed by '\n': the action/metadata line plus the document
// source line, each with its trailing newline.
func encodedBulkIndexRequestSize(req *elastic.BulkIndexRequest) (int64, error) {
	lines, err := req.Source()
	if err != nil {
		return 0, err
	}
	var size int64
	for _, line := range lines {
		size += int64(len(line)) + 1
	}
	return size, nil
}

func totalEncodedSize(requests []*eventIndexRequest) int64 {
	var total int64
	for _, req := range requests {
		total += req.encodedSize
	}
	return total
}

func newOversizedDocumentError(req *eventIndexRequest, limit int64) error {
	return firebolt.NewFBError(
		errorCodeDocTooLarge,
		fmt.Sprintf("elasticsearch: encoded document is %d bytes, larger than the %d byte limit; the document was not indexed", req.encodedSize, limit),
		firebolt.WithInfo(map[string]interface{}{
			"index":        req.Index,
			"doc_id":       req.DocID,
			"encoded_size": req.encodedSize,
			"byte_limit":   limit,
		}),
	)
}
