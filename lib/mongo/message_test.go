package mongo

import (
	"encoding/json"
	"fmt"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"testing"
	"time"

	transferMongo "github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/artie-labs/reader/config"
)

func TestParseMessagePartitionKey(t *testing.T) {
	{
		// Primary key as object ID
		objId, err := primitive.ObjectIDFromHex("507f1f77bcf86cd799439011")
		assert.NoError(t, err)
		msg, err := ParseMessage(bson.M{"_id": objId}, nil, "r")
		assert.NoError(t, err)
		assert.Equal(t, `{"$oid":"507f1f77bcf86cd799439011"}`, msg.pkMap["id"])

		rawMsg, err := msg.ToRawMessage(config.Collection{}, "database")
		assert.NoError(t, err)

		rawMsgBytes, err := json.Marshal(rawMsg.PartitionKey())
		assert.NoError(t, err)

		var dbz transferMongo.Debezium
		pkMap, err := dbz.GetPrimaryKey(rawMsgBytes, kafkalib.TopicConfig{CDCKeyFormat: kafkalib.JSONKeyFmt})
		assert.NoError(t, err)
		assert.Equal(t, "507f1f77bcf86cd799439011", pkMap["_id"])
	}
	{
		// Primary key as string
		msg, err := ParseMessage(bson.M{"_id": "hello world"}, nil, "r")
		assert.NoError(t, err)
		assert.Equal(t, "hello world", msg.pkMap["id"])
	}
	{
		// Primary key as ints
		for _, val := range []any{1001, int32(1002), int64(1003)} {
			msg, err := ParseMessage(bson.M{"_id": val}, nil, "r")
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprint(val), msg.pkMap["id"])
		}
	}
}

func TestParseMessage(t *testing.T) {
	objId, err := primitive.ObjectIDFromHex("507f1f77bcf86cd799439011")
	assert.NoError(t, err)

	decimal, err := primitive.ParseDecimal128("1234.5")
	assert.NoError(t, err)

	msg, err := ParseMessage(
		bson.M{
			"_id":     objId,
			"string":  "Hello, world!",
			"int32":   int32(42),
			"int64":   int64(3_000_000_000),
			"double":  3.14159,
			"decimal": decimal,
			"subDocument": bson.M{
				"nestedString": "Nested value",
			},
			"array": []any{"apple", "banana", "cherry"},
			"timestamp": primitive.Timestamp{
				T: uint32(1707856668), // Seconds since Unix epoch
				I: 123,                // Increment value
			},
			"datetime":   primitive.NewDateTimeFromTime(time.Date(2024, 2, 13, 20, 37, 48, 0, time.UTC)),
			"trueValue":  true,
			"falseValue": false,
			"nullValue":  nil,
		}, nil, "r")
	assert.NoError(t, err)

	rawMsg, err := msg.ToRawMessage(config.Collection{}, "database")
	assert.NoError(t, err)

	rawPkBytes, err := json.Marshal(rawMsg.PartitionKey())
	assert.NoError(t, err)

	var dbz transferMongo.Debezium
	pkMap, err := dbz.GetPrimaryKey(rawPkBytes, kafkalib.TopicConfig{CDCKeyFormat: kafkalib.JSONKeyFmt})
	assert.NoError(t, err)

	rawMsgBytes, err := json.Marshal(rawMsg.Event())
	assert.NoError(t, err)
	kvMap, err := dbz.GetEventFromBytes(rawMsgBytes)
	assert.NoError(t, err)

	expectedMap := map[string]any{
		"_id":         "507f1f77bcf86cd799439011",
		"string":      "Hello, world!",
		"int32":       int32(42),
		"int64":       int64(3000000000),
		"double":      3.14159,
		"decimal":     "1234.5",
		"subDocument": `{"nestedString":"Nested value"}`,
		"array":       []any{"apple", "banana", "cherry"},
		"datetime":    ext.NewExtendedTime(time.Date(2024, time.February, 13, 20, 37, 48, 0, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.999-07:00"),
		"trueValue":   true,
		"falseValue":  false,
		"nullValue":   nil,
	}

	actualKVMap, err := kvMap.GetData(pkMap, kafkalib.TopicConfig{})
	assert.NoError(t, err)
	for expectedKey, expectedVal := range expectedMap {
		actualVal, isOk := actualKVMap[expectedKey]
		assert.True(t, isOk, expectedKey)
		assert.Equal(t, expectedVal, actualVal, expectedKey)
	}
}
