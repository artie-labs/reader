package mongo

import (
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/debezium"
	"time"
)

func newRawMessage(msg mgoMessage, collection config.Collection, database string) (lib.RawMessage, error) {
	jsonExtendedString := string(msg.jsonExtendedBytes)
	evt := mongo.SchemaEventPayload{
		Schema: debezium.Schema{},
		Payload: mongo.Payload{
			After: &jsonExtendedString,
			Source: mongo.Source{
				Database:   database,
				Collection: collection.Name,
				TsMs:       time.Now().UnixMilli(),
			},
			Operation: "r",
		},
	}

	return lib.NewMongoMessage(
		collection.TopicSuffix(database),
		map[string]interface{}{"id": map[string]interface{}{"_id": msg.pk}}, evt,
	), nil
}
