package kafkalib

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/debezium"
)

type Message struct {
	topicSuffix        string
	partitionKeySchema debezium.FieldsObject
	partitionKeyValues map[string]any
	event              cdc.Event
}

func NewMessage(topicSuffix string, partitionKeySchema debezium.FieldsObject, partitionKeyValues map[string]any, event cdc.Event) Message {
	return Message{
		topicSuffix:        topicSuffix,
		partitionKeySchema: partitionKeySchema,
		partitionKeyValues: partitionKeyValues,
		event:              event,
	}
}

func (r Message) Topic(prefix string) string {
	if prefix == "" {
		return r.topicSuffix
	}

	return fmt.Sprintf("%s.%s", prefix, r.topicSuffix)
}

func (r Message) PartitionKey() debezium.PrimaryKeyPayload {
	return debezium.PrimaryKeyPayload{
		Schema:  r.partitionKeySchema,
		Payload: r.partitionKeyValues,
	}
}

func (r Message) PartitionKeyValues() map[string]any {
	return r.partitionKeyValues
}

func (r Message) Event() cdc.Event {
	return r.event
}
