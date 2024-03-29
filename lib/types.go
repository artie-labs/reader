package lib

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/util"
)

type RawMessage struct {
	TopicSuffix  string
	PartitionKey map[string]any
	payload      util.SchemaEventPayload
	mongoPayload mongo.SchemaEventPayload

	mongo bool
}

func NewRawMessage(topicSuffix string, partitionKey map[string]any, payload util.SchemaEventPayload) RawMessage {
	return RawMessage{
		TopicSuffix:  topicSuffix,
		PartitionKey: partitionKey,
		payload:      payload,
	}
}

func NewMongoMessage(topicSuffix string, partitionKey map[string]any, payload mongo.SchemaEventPayload) RawMessage {
	return RawMessage{
		TopicSuffix:  topicSuffix,
		PartitionKey: partitionKey,
		mongoPayload: payload,
		mongo:        true,
	}
}

func (r RawMessage) GetPayload() any {
	if r.mongo {
		return r.mongoPayload
	}

	return r.payload
}

type batchIterator[T any] struct {
	index   int
	batches [][]T
}

// Returns an iterator that produces multiple batches.
func NewBatchIterator[T any](batches [][]T) *batchIterator[T] {
	return &batchIterator[T]{batches: batches}
}

// Returns an iterator that produces a single batch.
func NewSingleBatchIterator[T any](batches []T) *batchIterator[T] {
	return NewBatchIterator([][]T{batches})
}

func (bi *batchIterator[T]) HasNext() bool {
	return bi.index < len(bi.batches)
}

func (bi *batchIterator[T]) Next() ([]T, error) {
	if !bi.HasNext() {
		return nil, fmt.Errorf("iterator has finished")
	}
	result := bi.batches[bi.index]
	bi.index++
	return result, nil
}
