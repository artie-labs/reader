package kafkalib

import (
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/debezium"
)

type Message struct {
	topicSuffix        string
	partitionKeySchema debezium.FieldsObject
	partitionKey       map[string]any
	event              cdc.Event
}

func NewMessage(topicSuffix string, partitionKeySchema debezium.FieldsObject, partitionKey map[string]any, event cdc.Event) Message {
	return Message{
		topicSuffix:        topicSuffix,
		partitionKeySchema: partitionKeySchema,
		partitionKey:       partitionKey,
		event:              event,
	}
}

func (r Message) TopicSuffix() string {
	return r.topicSuffix
}

func (r Message) PartitionKey() map[string]any {
	return r.partitionKey
}

func (r Message) PartitionKeySchema() debezium.FieldsObject {
	return r.partitionKeySchema
}

func (r Message) Event() cdc.Event {
	return r.event
}
