package lib

import (
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/util"
)

type RawMessage struct {
	TopicSuffix  string
	PartitionKey map[string]any
	payload      *util.SchemaEventPayload
	mongoPayload *mongo.SchemaEventPayload

	mongo bool
}

func NewRawMessage(topicSuffix string, partitionKey map[string]any, payload util.SchemaEventPayload) RawMessage {
	return RawMessage{
		TopicSuffix:  topicSuffix,
		PartitionKey: partitionKey,
		payload:      &payload,
	}
}

func NewMongoMessage(topicSuffix string, partitionKey map[string]any, payload mongo.SchemaEventPayload) RawMessage {
	return RawMessage{
		TopicSuffix:  topicSuffix,
		PartitionKey: partitionKey,
		mongoPayload: &payload,
		mongo:        true,
	}
}

func (r RawMessage) GetPayload() cdc.Event {
	if r.mongo {
		return r.mongoPayload
	}

	return r.payload
}
