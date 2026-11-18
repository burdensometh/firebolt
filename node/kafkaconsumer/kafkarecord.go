package kafkaconsumer

// KafkaRecord is the payload wrapper emitted by kafkaconsumer in manual acknowledgement mode.  It
// carries the raw message bytes plus the record's partition/offset so the executor can bind the
// event to its source position and the acker can track completion back to a committable offset.
//
// In the default automatic-offset mode the consumer continues to emit raw []byte payloads, so
// existing applications and nodes are unaffected unless they opt into manual acknowledgement.
type KafkaRecord struct {
	Value     []byte
	Partition int32
	Offset    int64
}

// SourcePartition implements firebolt.RecordPosition.
func (r *KafkaRecord) SourcePartition() int32 {
	return r.Partition
}

// SourceOffset implements firebolt.RecordPosition.
func (r *KafkaRecord) SourceOffset() int64 {
	return r.Offset
}
