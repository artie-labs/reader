package lib

import "github.com/artie-labs/transfer/lib/cdc/util"

type RawMessage struct {
	TopicSuffix  string
	PartitionKey map[string]interface{}
	Payload      util.SchemaEventPayload
}
