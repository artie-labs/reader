package lib

import "github.com/artie-labs/transfer/lib/cdc/util"

type RawMessage struct {
	TopicSuffix  string
	PartitionKey map[string]interface{}
	payload      util.SchemaEventPayload
}

func NewRawMessage(topicSuffix string, partitionKey map[string]interface{}, payload util.SchemaEventPayload) RawMessage {
	return RawMessage{
		TopicSuffix:  topicSuffix,
		PartitionKey: partitionKey,
		payload:      payload,
	}
}

func (r RawMessage) GetPayload() interface{} {
	return r.payload
}
