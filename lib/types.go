package lib

import (
	"github.com/artie-labs/transfer/lib/cdc"
)

type RawMessage struct {
	topicSuffix  string
	partitionKey map[string]any
	event        cdc.Event
}

func NewRawMessage(topicSuffix string, partitionKey map[string]any, event cdc.Event) RawMessage {
	return RawMessage{
		topicSuffix:  topicSuffix,
		partitionKey: partitionKey,
		event:        event,
	}
}

func (r RawMessage) TopicSuffix() string {
	return r.topicSuffix
}

func (r RawMessage) PartitionKey() map[string]any {
	return r.partitionKey
}

func (r RawMessage) Event() cdc.Event {
	return r.event
}
