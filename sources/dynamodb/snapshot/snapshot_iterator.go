package snapshot

import (
	"fmt"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

type Iterator struct {
	ch        chan map[string]types.AttributeValue
	keys      []string
	tableName string
	batchSize int32
	done      bool
}

func NewSnapshotIterator(ch chan map[string]types.AttributeValue, keys []string, tblName string, batchSize int32) *Iterator {
	return &Iterator{
		ch:        ch,
		keys:      keys,
		tableName: tblName,
		batchSize: batchSize,
	}
}

func (s *Iterator) HasNext() bool {
	return !s.done
}

func (s *Iterator) Next() ([]lib.RawMessage, error) {
	var msgs []lib.RawMessage
	for msg := range s.ch {
		dynamoMsg, err := dynamo.NewMessageFromExport(msg, s.keys, s.tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to cast message from DynamoDB, msg: %v, err: %w", msg, err)
		}

		msgs = append(msgs, dynamoMsg.RawMessage())
		if int32(len(msgs)) >= s.batchSize {
			return msgs, nil
		}
	}

	s.done = true
	return msgs, nil
}
