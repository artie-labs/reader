package lib

import (
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/debezium"
)

type RawMessage struct {
	topicSuffix        string
	partitionKeySchema debezium.FieldsObject
	partitionKey       map[string]any
	event              cdc.Event
}

func NewRawMessage(topicSuffix string, partitionKeySchema debezium.FieldsObject, partitionKey map[string]any, event cdc.Event) RawMessage {
	return RawMessage{
		topicSuffix:        topicSuffix,
		partitionKeySchema: partitionKeySchema,
		partitionKey:       partitionKey,
		event:              event,
	}
}

func (r RawMessage) TopicSuffix() string {
	return r.topicSuffix
}

func (r RawMessage) PartitionKey() map[string]any {
	return r.partitionKey
}

func (r RawMessage) PartitionKeySchema() debezium.FieldsObject {
	return r.partitionKeySchema
}

func (r RawMessage) Event() cdc.Event {
	return r.event
}
