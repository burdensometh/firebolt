package kafkaconsumer

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/ack"
	"github.com/digitalocean/firebolt/metrics"
)

func TestManualOffsetCommitConfig(t *testing.T) {
	kc := &KafkaConsumer{}

	// default: auto-commit enabled when manual mode is off
	cfg := createValidConfig()
	cm, err := kc.buildConfigMap(cfg)
	assert.NoError(t, err)
	assert.Equal(t, true, (*cm)["enable.auto.commit"])

	// manual mode turns auto-commit off
	kc.manualOffsetCommit = true
	cm, err = kc.buildConfigMap(cfg)
	assert.NoError(t, err)
	assert.Equal(t, false, (*cm)["enable.auto.commit"])
}

func TestCheckConfigManualOffsetCommit(t *testing.T) {
	kc := &KafkaConsumer{}

	cfg := createValidConfig()
	cfg["manualoffsetcommit"] = "not-a-bool"
	err := kc.checkConfig(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manualoffsetcommit")

	cfg = createValidConfig()
	cfg["manualoffsetcommit"] = "true"
	assert.NoError(t, kc.checkConfig(cfg))
}

func TestProcessMessageManualAckWrapsRecord(t *testing.T) {
	metrics.Init("kafkaconsumer_ack_test")

	kc := &KafkaConsumer{
		manualOffsetCommit: true,
		acker:              ack.NewAcker(true, nil),
		topic:              "unit-test",
		metrics:            buildTestMetrics(),
	}
	ch := make(chan firebolt.Event, 10)
	kc.sendCh = ch

	topic := "unit-test"
	kc.processMessage(&kafka.Message{
		Value:          []byte("hello"),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2, Offset: kafka.Offset(42)},
	})

	ev := <-ch
	record, ok := ev.Payload.(*KafkaRecord)
	assert.True(t, ok)
	assert.Equal(t, []byte("hello"), record.Value)
	assert.Equal(t, int32(2), record.SourcePartition())
	assert.Equal(t, int64(42), record.SourceOffset())
}

func TestProcessMessageAutoCommitEmitsRawBytes(t *testing.T) {
	metrics.Init("kafkaconsumer_ack_test")

	kc := &KafkaConsumer{
		manualOffsetCommit: false,
		acker:              ack.NewAcker(false, nil),
		topic:              "unit-test",
		metrics:            buildTestMetrics(),
	}
	ch := make(chan firebolt.Event, 10)
	kc.sendCh = ch

	topic := "unit-test"
	kc.processMessage(&kafka.Message{
		Value:          []byte("hello"),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2, Offset: kafka.Offset(42)},
	})

	ev := <-ch
	raw, ok := ev.Payload.([]byte)
	assert.True(t, ok)
	assert.Equal(t, []byte("hello"), raw)
}

// TestManualAckCommitOnlyContiguousPrefix wires the consumer's real commit callback to a mock
// consumer and proves only the contiguous completed prefix is committed, never jumping a hole.
func TestManualAckCommitOnlyContiguousPrefix(t *testing.T) {
	metrics.Init("kafkaconsumer_ack_test")

	mockConsumer := &kafkaMockCommitter{}
	kc := &KafkaConsumer{
		topic:              "unit-test",
		consumer:           mockConsumer,
		manualOffsetCommit: true,
	}
	kc.acker = ack.NewAcker(true, kc.commitAckedOffset)
	kc.acker.AssignPartition(0, 0)

	tr0 := kc.acker.NewRecordTracker(0, 0)
	tr1 := kc.acker.NewRecordTracker(0, 1)
	tr2 := kc.acker.NewRecordTracker(0, 2)
	tr0.EnterRoot()
	tr1.EnterRoot()
	tr2.EnterRoot()

	// record 1 completes first: behind the hole at 0, no commit
	tr1.Terminal()
	assert.Empty(t, mockConsumer.committed)

	// record 0 completes: prefix through 1 becomes committable (next offset = 2)
	tr0.Terminal()
	assert.Equal(t, []int64{2}, mockConsumer.committed)

	// record 2 completes: next offset = 3
	tr2.Terminal()
	assert.Equal(t, []int64{2, 3}, mockConsumer.committed)
}

// TestManualAckRebalanceFencesStaleCommit proves that after a rebalance fences a generation, late
// completion callbacks from the old assignment cannot commit against the new generation.
func TestManualAckRebalanceFencesStaleCommit(t *testing.T) {
	metrics.Init("kafkaconsumer_ack_test")

	mockConsumer := &kafkaMockCommitter{}
	kc := &KafkaConsumer{
		topic:              "unit-test",
		consumer:           mockConsumer,
		manualOffsetCommit: true,
	}
	kc.acker = ack.NewAcker(true, kc.commitAckedOffset)
	kc.acker.AssignPartition(0, 0)

	oldTracker := kc.acker.NewRecordTracker(0, 0)
	oldTracker.EnterRoot()

	// rebalance: revoke then reassign; the old generation is fenced
	kc.acker.RevokePartition(0)
	kc.acker.AssignPartition(0, 0)

	// late callback from a branch that belonged to the revoked generation
	oldTracker.Terminal()
	assert.Empty(t, mockConsumer.committed, "stale generation must not commit")

	// a fresh record under the new generation commits normally
	newTracker := kc.acker.NewRecordTracker(0, 0)
	newTracker.EnterRoot()
	newTracker.Terminal()
	assert.Equal(t, []int64{1}, mockConsumer.committed)
}

// kafkaMockCommitter is a minimal stand-in satisfying only the commit path used by these tests.
type kafkaMockCommitter struct {
	committed []int64
}

func (m *kafkaMockCommitter) Subscribe(string, kafka.RebalanceCb) error { panic("not used") }
func (m *kafkaMockCommitter) Events() chan kafka.Event                  { panic("not used") }
func (m *kafkaMockCommitter) Assign([]kafka.TopicPartition) error       { panic("not used") }
func (m *kafkaMockCommitter) Unassign() error                           { panic("not used") }
func (m *kafkaMockCommitter) Committed([]kafka.TopicPartition, int) ([]kafka.TopicPartition, error) {
	panic("not used")
}
func (m *kafkaMockCommitter) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	for _, tp := range offsets {
		m.committed = append(m.committed, int64(tp.Offset))
	}
	return offsets, nil
}
func (m *kafkaMockCommitter) QueryWatermarkOffsets(string, int32, int) (int64, int64, error) {
	panic("not used")
}
func (m *kafkaMockCommitter) GetMetadata(*string, bool, int) (*kafka.Metadata, error) {
	panic("not used")
}
func (m *kafkaMockCommitter) Close() error { panic("not used") }

func buildTestMetrics() *Metrics {
	m := &Metrics{}
	m.RegisterConsumerMetrics()
	return m
}
