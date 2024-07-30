package mongo

import (
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/debezium"
	"go.mongodb.org/mongo-driver/bson"
)

type Message struct {
	jsonExtendedString string
	operation          string
	pkMap              map[string]any
}

func (m *Message) ToRawMessage(collection config.Collection, database string) (lib.RawMessage, error) {
	evt := &mongo.SchemaEventPayload{
		Schema: debezium.Schema{},
		Payload: mongo.Payload{
			After: &m.jsonExtendedString,
			Source: mongo.Source{
				Database:   database,
				Collection: collection.Name,
				TsMs:       time.Now().UnixMilli(),
			},
			Operation: m.operation,
		},
	}

	pkMap := map[string]any{
		"payload": m.pkMap,
	}

	return lib.NewRawMessage(collection.TopicSuffix(database), pkMap, evt), nil
}

func ParseMessage(result bson.M, op string) (*Message, error) {
	jsonExtendedBytes, err := bson.MarshalExtJSON(result, false, false)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document to JSON extended: %w", err)
	}

	bsonPk, isOk := result["_id"]
	if !isOk {
		return nil, fmt.Errorf("failed to get partition key, row: %v", result)
	}

	var idString string
	switch castedPk := bsonPk.(type) {
	case primitive.ObjectID:
		idString = fmt.Sprintf(`{"$oid":"%s"}`, castedPk.Hex())
	case string:
		idString = castedPk
	case int, int32, int64:
		idString = fmt.Sprintf("%d", castedPk)
	default:
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

		idString = string(pkBytes)
	}

	return &Message{
		jsonExtendedString: string(jsonExtendedBytes),
		operation:          op,
		pkMap: map[string]any{
			"id": idString,
		},
	}, nil
}
