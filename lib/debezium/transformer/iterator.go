package transformer

import (
	"fmt"
	"github.com/artie-labs/reader/lib"
)

type DebeziumIterator struct {
	debeziumTransformer DebeziumTransformer
	adapter             Adapter
	iter                RowsIterator
}

func NewDebeziumIterator(adapter Adapter) (DebeziumIterator, error) {
	iter, err := adapter.NewIterator()
	if err != nil {
		return DebeziumIterator, fmt.Errorf("failed to create iterator :%w", err)
	}

	return newDebeziumIteratorWithIterator(adapter, iter), nil
}

func newDebeziumIteratorWithIterator(adapter Adapter, iter RowsIterator) DebeziumIterator {
	return DebeziumIterator{
		debeziumTransformer: NewDebeziumTransformer(adapter.TableName(), adapter.FieldConverters(), adapter.PartitionKeys(), adapter.TopicSuffix()),
		adapter:             adapter,
		iter:                iter,
	}
}

func (d *DebeziumIterator) HasNext() bool {
	return d != nil && d.iter.HasNext()
}

func (d *DebeziumIterator) Next() ([]lib.RawMessage, error) {
	if !d.HasNext() {
		return make([]lib.RawMessage, 0), nil
	}

	rows, err := d.iter.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to scan: %w", err)
	}

	var result []lib.RawMessage
	for _, row := range rows {
		payload, err := d.debeziumTransformer.createPayload(row)
		if err != nil {
			return nil, fmt.Errorf("failed to create Debezium payload: %w", err)
		}

		result = append(result, lib.NewRawMessage(d.adapter.TopicSuffix(), d.debeziumTransformer.partitionKey(row), &payload))
	}

	return result, nil
}
