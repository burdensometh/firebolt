package elasticsearch

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
)

// IndexRequest is the event payload type to use when passing data to the elasticsearch node.
type IndexRequest struct {
	Index       string      `json:"index"`
	MappingType string      `json:"mapping_type"` // in ES 7.x+, leave MappingType unset and ES will use `_doc`
	DocID       string      `json:"doc_id"`
	Doc         interface{} `json:"doc"`
}

// Elasticsearch is a Node that uses elastic_index_client to index documents in the configured ElasticSearch cluster.
type Elasticsearch struct {
	fbcontext.ContextAware
	serviceFactory bulkServiceFactory
	indexClient    *ElasticIndexClient
	done           context.CancelFunc
}

// Setup is a no-op in index
func (i *Elasticsearch) Setup(cfgMap map[string]string) error {
	ctx, done := context.WithCancel(context.Background())
	i.done = done

	config := firebolt.Nodeconfig(cfgMap)
	esURL, err := config.StringConfigRequired("elastic-addr")
	if err != nil {
		return err
	}

	esUsername, err := config.StringConfig("elastic-username", "")
	if err != nil {
		return err
	}

	esPassword, err := config.StringConfig("elastic-password", "")
	if err != nil {
		return err
	}

	batchSize, err := config.IntConfig("batch-size", 100, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	batchMaxWaitMs, err := config.IntConfig("batch-max-wait-ms", 1000, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	bulkIndexTimeoutMs, err := config.IntConfig("bulk-index-timeout-ms", 5000, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	reconnectBatchCount, err := config.IntConfig("reconnect-batch-count", 10000, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	bulkIndexMaxRetries, err := config.IntConfig("bulk-index-max-retries", 3, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	bulkIndexTimeoutSeconds, err := config.IntConfig("bulk-index-timeout-seconds", 20, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	indexWorkers, err := config.IntConfig("index-workers", 1, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	// the maximum encoded size, including action metadata and newlines, of a
	// single bulk request body sent to Elasticsearch (default 10 MiB)
	bulkMaxRequestBytes, err := config.IntConfig("bulk-max-request-bytes", 10*1024*1024, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	// total encoded bytes that may be buffered anywhere in this node: requests
	// waiting to be batched, queued/in-flight bulk requests, and retries.  When
	// this budget is exhausted Send blocks, applying backpressure upstream
	// instead of accumulating unbounded goroutines or queues.
	defaultBufferedMemoryBytes := 10 * bulkMaxRequestBytes
	if defaultBufferedMemoryBytes > math.MaxInt32 || defaultBufferedMemoryBytes < bulkMaxRequestBytes {
		defaultBufferedMemoryBytes = math.MaxInt32
	}
	bufferedMemoryBytes, err := config.IntConfig("buffered-memory-bytes", defaultBufferedMemoryBytes, bulkMaxRequestBytes, math.MaxInt32)
	if err != nil {
		return err
	}

	bulkProcessHistogramMin, err := config.Float64Config("histogram-min-bucket-sec", 0.01, 0.01, math.MaxFloat64)
	if err != nil {
		return err
	}

	bulkProcessHistogramMax, err := config.Float64Config("histogram-max-bucket-sec", float64(2*(bulkIndexTimeoutMs/1000)), 0.01, math.MaxFloat64)
	if err != nil {
		return err
	}

	bulkProcessingHistogramBuckets, err := config.IntConfig("histogram-bucket-count", 8, 1, math.MaxInt)
	if err != nil {
		return err
	}

	// initialize metrics
	metrics := &Metrics{}
	metrics.RegisterElasticIndexMetrics(bulkProcessHistogramMin, bulkProcessHistogramMax, bulkProcessingHistogramBuckets)

	// service factory; in tests it must be prepopulated with a mock
	if i.serviceFactory == nil {
		i.serviceFactory = newEsBulkServiceFactory(ctx, esURL, esUsername, esPassword, reconnectBatchCount, bulkIndexTimeoutMs, metrics)
	}

	i.indexClient = NewElasticIndexClient(
		ctx,
		i.serviceFactory,
		metrics,
		batchSize,
		bulkIndexMaxRetries,
		bulkIndexTimeoutSeconds,
		indexWorkers,
		time.Duration(batchMaxWaitMs)*time.Millisecond,
		int64(bulkMaxRequestBytes),
		int64(bufferedMemoryBytes))

	go i.indexClient.Run(ctx)

	return nil
}

// ProcessAsync enqueues the document index request for bulk indexing
func (i *Elasticsearch) ProcessAsync(event *firebolt.AsyncEvent) {
	indexRequest, ok := event.Payload.(IndexRequest)
	if !ok {
		event.ReturnError(errors.New("failed type assertion for conversion to IndexRequest"))
		return
	}

	ir := &eventIndexRequest{
		Index:       indexRequest.Index,
		MappingType: indexRequest.MappingType,
		Doc:         indexRequest.Doc,
		DocID:       indexRequest.DocID,
		Event:       event,
	}

	// Send only returns an error for documents rejected at admission
	// (unmarshallable or individually over the byte limits); those are returned
	// to the caller immediately and never block the documents after them.
	if err := i.indexClient.Send(ir); err != nil {
		event.ReturnError(err)
	}
}

// Receive handles a message from another node or an external source
func (i *Elasticsearch) Receive(msg fbcontext.Message) error {
	return errors.New("elasticsearch: message not supported")
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (i *Elasticsearch) Shutdown() error {
	i.done()
	return nil
}
