package lib

import (
	"github.com/artie-labs/transfer/lib/cdc"
)

type RawMessage struct {
	TopicSuffix  string
	PartitionKey map[string]any
	event        cdc.Event
}

func NewRawMessage(topicSuffix string, partitionKey map[string]any, event cdc.Event) RawMessage {
	return RawMessage{
		TopicSuffix:  topicSuffix,
		PartitionKey: partitionKey,
		event:        event,
	}
}

func (r RawMessage) GetEvent() cdc.Event {
	return r.event
}
