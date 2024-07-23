package transfer

import (
	"testing"

	transferCfg "github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources/mongo"
)

func TestWriter_MessageToEvent(t *testing.T) {
	objId, err := primitive.ObjectIDFromHex("507f1f77bcf86cd799439011")
	assert.NoError(t, err)

	msg, err := mongo.ParseMessage(bson.M{
		"_id":    objId,
		"string": "Hello, world!",
		"int64":  int64(1234567890),
		"double": 3.14159,
	}, "r")
	assert.NoError(t, err)

	message, err := msg.ToRawMessage(config.Collection{Name: "collection"}, "database")
	assert.NoError(t, err)

	writer := Writer{
		cfg: transferCfg.Config{
			SharedTransferConfig: transferCfg.SharedTransferConfig{},
		},
		tc: &kafkalib.TopicConfig{
			CDCKeyFormat: kafkalib.JSONKeyFmt,
		},
	}

	evtOut, err := writer.messageToEvent(message)
	assert.NoError(t, err)

	for expectedKey, expectedValue := range map[string]any{
		"__artie_delete":          false,
		"__artie_only_set_delete": false,
		"_id":                     "507f1f77bcf86cd799439011",
		"double":                  3.14159,
		"int64":                   int32(1234567890),
		"string":                  "Hello, world!",
	} {
		actualValue, isOk := evtOut.Data[expectedKey]
		assert.True(t, isOk, expectedKey)
		assert.Equal(t, expectedValue, actualValue, expectedKey)

		delete(evtOut.Data, expectedKey)
	}

	assert.Empty(t, evtOut.Data)
	assert.Equal(t, map[string]any{"_id": objId.Hex()}, evtOut.PrimaryKeyMap)
}
