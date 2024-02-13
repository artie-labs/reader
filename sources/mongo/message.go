package mongo

import (
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/debezium"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

type mgoMessage struct {
	jsonExtendedBytes []byte
	pk                interface{}
}

func (m *mgoMessage) toRawMessage(collection config.Collection, database string) (lib.RawMessage, error) {
	jsonExtendedString := string(m.jsonExtendedBytes)
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
		map[string]interface{}{"id": map[string]interface{}{"_id": m.pk}}, evt,
	), nil
}

func parseMessage(result bson.M) (*mgoMessage, error) {
	jsonExtendedBytes, err := bson.MarshalExtJSON(result, false, false)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document to JSON extended: %w", err)
	}

	var jsonExtendedMap map[string]interface{}
	if err = bson.UnmarshalExtJSON(jsonExtendedBytes, false, &jsonExtendedMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON extended to map: %w", err)
	}

	pk, isOk := jsonExtendedMap["_id"]
	if !isOk {
		return nil, fmt.Errorf("failed to get partition key, row: %v", jsonExtendedMap)
	}

	return &mgoMessage{
		jsonExtendedBytes: jsonExtendedBytes,
		pk:                pk,
	}, nil
}
