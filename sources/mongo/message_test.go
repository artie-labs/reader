package mongo

import (
	"encoding/json"
	"github.com/artie-labs/reader/config"
	transferMongo "github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
)

func TestParseMessagePartitionKey(t *testing.T) {
	objId, err := primitive.ObjectIDFromHex("507f1f77bcf86cd799439011")
	assert.NoError(t, err)
	msg, err := parseMessage(bson.M{
		"key": "value",
		"_id": objId,
	})
	assert.NoError(t, err)
	assert.Equal(t, `{"$oid":"507f1f77bcf86cd799439011"}`, msg.pkMap["id"])

	rawMsg, err := msg.toRawMessage(config.Collection{}, "database")
	assert.NoError(t, err)

	rawMsgBytes, err := json.Marshal(rawMsg.PartitionKey)
	assert.NoError(t, err)

	var dbz transferMongo.Debezium
	pkMap, err := dbz.GetPrimaryKey(rawMsgBytes, &kafkalib.TopicConfig{CDCKeyFormat: kafkalib.JSONKeyFmt})
	assert.NoError(t, err)
	assert.Equal(t, "507f1f77bcf86cd799439011", pkMap["_id"])
}
