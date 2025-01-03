package mongo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
)

type Message struct {
	afterJSONExtendedString  string
	beforeJSONExtendedString *string
	operation                string
	pkMap                    map[string]any
}

func (m *Message) ToRawMessage(collection config.Collection, database string) (kafkalib.Message, error) {
	evt := &mongo.SchemaEventPayload{
		Schema: debezium.Schema{},
		Payload: mongo.Payload{
			After:  &m.afterJSONExtendedString,
			Before: m.beforeJSONExtendedString,
			Source: mongo.Source{
				Database:   database,
				Collection: collection.Name,
				TsMs:       time.Now().UnixMilli(),
			},
			Operation: m.operation,
		},
	}
	// MongoDB wouldn't include the schema.
	return kafkalib.NewMessage(collection.TopicSuffix(database), debezium.FieldsObject{}, m.pkMap, evt), nil
}

func ParseMessage(after bson.M, before *bson.M, op string) (*Message, error) {
	bsonPk, isOk := after["_id"]
	if !isOk {
		return nil, fmt.Errorf("failed to get partition key, row: %v", after)
	}

	// When canonical is enabled, it will emphasize type preservation
	afterRow, err := bson.MarshalExtJSON(after, true, false)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document to JSON extended: %w", err)
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
		if err = json.Unmarshal(afterRow, &jsonExtendedMap); err != nil {
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

	msg := &Message{
		afterJSONExtendedString: string(afterRow),
		operation:               op,
		pkMap: map[string]any{
			"id": idString,
		},
	}

	if before != nil {
		beforeRow, err := bson.MarshalExtJSON(*before, true, false)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document to JSON extended: %w", err)
		}

		msg.beforeJSONExtendedString = typing.ToPtr(string(beforeRow))
	}

	return msg, nil
}
