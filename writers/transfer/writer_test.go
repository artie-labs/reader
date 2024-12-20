package transfer

import (
	"fmt"
	"testing"

	transferCfg "github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/mongo"
)

func generateBasicColumns(n uint) []columns.Column {
	cols := make([]columns.Column, n)
	for i := range cols {
		cols[i] = columns.NewColumn(fmt.Sprintf("col-%d", i), typing.String)
	}

	return cols
}

func assertOneColumn(t *testing.T, expected columns.Column, actual []columns.Column) {
	assert.Len(t, actual, 1)
	assert.Equal(t, expected, actual[0])
}

func TestBuildColumns(t *testing.T) {
	{
		// TopicConfig is not set
		basicCols := generateBasicColumns(3)
		assert.Equal(t, basicCols, buildColumns(basicCols, kafkalib.TopicConfig{}))
	}
	{
		// IncludeArtieUpdatedAt = true
		basicCols := generateBasicColumns(3)
		cols := buildColumns(basicCols, kafkalib.TopicConfig{
			IncludeArtieUpdatedAt: true,
		})

		assert.Equal(t, basicCols, cols[:3])
		assertOneColumn(t, columns.NewColumn("__artie_updated_at", typing.TimestampTZ), cols[3:])
	}
	{
		// IncludeDatabaseUpdatedAt = true
		basicCols := generateBasicColumns(3)
		cols := buildColumns(basicCols, kafkalib.TopicConfig{
			IncludeDatabaseUpdatedAt: true,
		})

		assert.Equal(t, basicCols, cols[:3])
		assertOneColumn(t, columns.NewColumn("__artie_db_updated_at", typing.TimestampTZ), cols[3:])
	}
	{
		// SoftDelete = true
		basicCols := generateBasicColumns(3)
		cols := buildColumns(basicCols, kafkalib.TopicConfig{
			SoftDelete: true,
		})

		assert.Equal(t, basicCols, cols[:3])
		assertOneColumn(t, columns.NewColumn("__artie_delete", typing.Boolean), cols[3:])
	}
}

func TestWriter_MessageToEvent(t *testing.T) {
	objId, err := primitive.ObjectIDFromHex("507f1f77bcf86cd799439011")
	assert.NoError(t, err)

	msg, err := mongo.ParseMessage(bson.M{
		"_id":    objId,
		"string": "Hello, world!",
		"int64":  int64(1234567890),
		"double": 3.14159,
	}, nil, "r")
	assert.NoError(t, err)

	message, err := msg.ToRawMessage(config.Collection{Name: "collection"}, "database")
	assert.NoError(t, err)

	writer := Writer{
		cfg: transferCfg.Config{},
		tc:  kafkalib.TopicConfig{CDCKeyFormat: kafkalib.JSONKeyFmt},
	}

	evtOut, err := writer.messageToEvent(message)
	assert.NoError(t, err)

	for expectedKey, expectedValue := range map[string]any{
		"__artie_delete":          false,
		"__artie_only_set_delete": false,
		"_id":                     "507f1f77bcf86cd799439011",
		"double":                  3.14159,
		"int64":                   int64(1234567890),
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
