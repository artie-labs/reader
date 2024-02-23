package mongo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/debezium"
	"go.mongodb.org/mongo-driver/bson"
)

type mgoMessage struct {
	jsonExtendedString string
	pkMap              map[string]any
}

func (m *mgoMessage) toRawMessage(collection config.Collection, database string) (lib.RawMessage, error) {
	evt := mongo.SchemaEventPayload{
		Schema: debezium.Schema{},
		Payload: mongo.Payload{
			After: &m.jsonExtendedString,
			Source: mongo.Source{
				Database:   database,
				Collection: collection.Name,
				TsMs:       time.Now().UnixMilli(),
			},
			Operation: "r",
		},
	}

	pkMap := map[string]any{
		"payload": m.pkMap,
	}

	return lib.NewMongoMessage(collection.TopicSuffix(database), pkMap, evt), nil
}

func parseMessage(result bson.M) (*mgoMessage, error) {
	jsonExtendedBytes, err := bson.MarshalExtJSON(result, false, false)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document to JSON extended: %w", err)
	}

	var jsonExtendedMap map[string]any
	if err = json.Unmarshal(jsonExtendedBytes, &jsonExtendedMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON extended to map: %w", err)
	}

	pk, isOk := jsonExtendedMap["_id"]
	if !isOk {
		return nil, fmt.Errorf("failed to get partition key, row: %v", jsonExtendedMap)
	}

	pkBytes, err := json.Marshal(pk)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ext json: %w", err)
	}
	return &mgoMessage{
		jsonExtendedString: string(jsonExtendedBytes),
		pkMap: map[string]any{
			"id": string(pkBytes),
		},
	}, nil
}
