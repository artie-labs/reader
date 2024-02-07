package mongo

import (
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"time"
)

func newRawMessage(rowMessage map[string]interface{}, collection config.Collection) (lib.RawMessage, error) {
	msg := util.SchemaEventPayload{
		Schema: debezium.Schema{},
		Payload: util.Payload{
			After: rowMessage,
			Source: util.Source{
				Table: collection.Name,
				TsMs:  time.Now().UnixMilli(),
			},
			Operation: "r",
		},
	}

	partitionKey, isOk := rowMessage["_id"]
	if !isOk {
		return lib.RawMessage{}, fmt.Errorf("failed to get partition key, row: %v", rowMessage)
	}

	return lib.RawMessage{
		TopicSuffix: collection.TopicSuffix(),
		PartitionKey: map[string]interface{}{
			"id": map[string]interface{}{
				"_id": partitionKey,
			},
		},
		Payload: msg,
	}, nil
}
