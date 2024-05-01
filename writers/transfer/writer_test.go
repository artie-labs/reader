package transfer

import (
	"context"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mocks"
	"github.com/artie-labs/transfer/lib/cdc/util"
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
	})
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
	assert.Equal(t, map[string]any{
		"__artie_delete": false,
		"_id":            "507f1f77bcf86cd799439011",
		"double":         3.14159,
		"int64":          1.23456789e+09,
		"string":         "Hello, world!",
	}, evtOut.Data)

	assert.Equal(t, map[string]any{"_id": objId.Hex()}, evtOut.PrimaryKeyMap)
}

func TestWriter_Write(t *testing.T) {
	var rawMsgs []lib.RawMessage
	for range 100 {
		rawMsgs = append(rawMsgs, lib.NewRawMessage(
			"topic-suffix",
			map[string]any{"key": "value"},
			&util.SchemaEventPayload{
				Payload: util.Payload{
					After: map[string]any{"a": "b"},
					Source: util.Source{
						TsMs:  1000,
						Table: "table",
					},
					Operation: "c",
				},
			},
		))
	}

	writer, err := NewWriter(transferCfg.Config{
		Mode:   transferCfg.Replication,
		Output: "test",
		Kafka: &transferCfg.Kafka{
			TopicConfigs: []*kafkalib.TopicConfig{
				{
					TableName: "table",
				},
			},
		},
	}, &mocks.FakeClient{})
	assert.NoError(t, err)

	assert.Nil(t, writer.primaryKeys)
	assert.NoError(t, writer.Write(context.Background(), rawMsgs))
	assert.Len(t, writer.primaryKeys, 1)
	assert.Equal(t, "key", writer.primaryKeys[0])
}
