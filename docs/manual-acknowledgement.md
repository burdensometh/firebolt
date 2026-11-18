# Manual Acknowledgement & Offset Commit

By default the built-in `kafkaconsumer` source relies on librdkafka's periodic **automatic offset
commit**.  Because node processing - especially asynchronous nodes such as the bulk
`elasticsearch` indexer - completes after the consumer has already moved on, an auto-committed
offset can run ahead of what has actually been persisted downstream.  A crash in that window
silently skips records.

Setting the source parameter `manualoffsetcommit: true` switches the source to **manual
acknowledgement**.  The consumer then commits an offset only after a source record has fully
traversed the processing tree.

## What "acknowledged" means

A single Kafka message may fan out to several node branches, be filtered, fail into an error
handler, or pass through asynchronous nodes that return long after the original goroutine has
moved on.  Firebolt tracks the record with a branch counter:

* the record enters the root node with one open branch;
* each additional child delivery, and every event produced by a fanout node, opens a branch;
* a branch closes at a **terminal outcome**:
  * a successful result at a leaf node (a node with no children),
  * a node filtering the event (returning `nil`),
  * a failure at a node that has no `error_handler`, or
  * an error handler reaching its own terminal outcome (the failing node's branch is transferred
    to the error handler rather than duplicated),
  * a `discard_on_full_buffer` discard, which is a declared terminal policy.

The record is acknowledged only when its **last** open branch closes.  Until then its offset is
held uncommitted regardless of how far the consumer has fetched ahead.

## Contiguous-prefix commits and out-of-order completion

Records complete out of order whenever branches take different amounts of time (for example one
document indexes slowly while later documents succeed quickly).  For each partition Firebolt keeps
the longest **contiguous completed prefix** and commits only that:

* if offsets `0..4` and `6` are complete but `5` is still in flight, the committed position stays
  at offset `5`; offset `6` is buffered internally and cannot advance the commit past the hole;
* once offset `5` completes, the prefix extends and a single commit catches up.

Committed offsets use Kafka's "next offset to consume" convention, so completing offset *n* commits
*n+1*.

## Rebalances and generation fencing

Every partition assignment gets a monotonically increasing **generation**.  On a rebalance the
revoked generation is fenced before the partitions are released:

* completion callbacks that arrive late from work belonging to the old generation are discarded
  and can never issue a commit;
* a callback cannot commit against a newer generation owned by this instance or another consumer;
* buffered unacknowledged state is dropped from memory.

## Shutdown and replay

On clean shutdown (and on rebalance) unacknowledged records are **not** committed.  When the
partition is next assigned - to this instance after restart or to another instance in the consumer
group - consumption resumes from the last committed Kafka offset and those records are replayed.

## Delivery semantics

Manual acknowledgement provides **at-least-once** delivery: a record whose acknowledgement never
reaches Kafka before a failure is processed again.  Replays can therefore produce duplicate
downstream effects, so sinks must be idempotent (the `elasticsearch` node supports this through
its configurable document `DocID`).

Firebolt does **not** provide an atomic transaction spanning Kafka and any downstream system.
Committing a Kafka offset never commits Elasticsearch writes (or any other sink) atomically, and
vice versa; the guarantee is only that unacknowledged source records will be redelivered.

## Payload wrapping

In manual mode the source emits a `*kafkaconsumer.KafkaRecord` payload (which carries the raw
message bytes plus the partition and offset) instead of a bare `[]byte`.  A root node that
performs a `[]byte` type assertion must accept the wrapper and read `KafkaRecord.Value`, or a
conversion node should be placed first in the hierarchy.  Auto-commit mode continues to emit
`[]byte` unchanged.

## Configuration

```yaml
source:
  name: kafkaconsumer
  params:
    brokers: kafka01:9092,kafka02:9092
    consumergroup: testapp
    topic: logs-all
    buffersize: 1000
    manualoffsetcommit: true   # [default: false] commit offsets only after all terminal branches complete
```

When the flag is omitted or `false`, behavior is unchanged: librdkafka auto-commit stays enabled
and no acknowledgement tracking is attached to events.

## Synchronous and asynchronous nodes

Both node styles are supported without application changes:

* **synchronous / fanout nodes** - branch accounting is updated as `Process` returns, including
  filtered results, errors, and fanout slices;
* **asynchronous nodes** (including the built-in `elasticsearch` bulk indexer) - the branch stays
  open until the node invokes `ReturnEvent`, `ReturnFiltered`, or `ReturnError`, so an index that
  is still buffered in an ES bulk batch keeps the originating Kafka offset uncommitted.  An ES
  document rejected as a permanent, non-retryable mapping error is returned via `ReturnError` and
  follows the node's declared error-handler / terminal-failure policy rather than blocking the
  partition forever.
