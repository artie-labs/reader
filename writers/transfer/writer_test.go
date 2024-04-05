package transfer

import (
	"testing"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestToJSONTypes(t *testing.T) {
	{
		// Empty map.
		dataIn := map[string]any{}
		dataOut, err := toJSONTypes(dataIn)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{}, dataOut)
	}
	{
		// Non-empty map.
		dataIn := map[string]any{
			"nil":     nil,
			"int":     12345,
			"int64":   int64(123456),
			"float32": float32(12),
			"float64": float32(23),
			"binary":  []byte{byte(0), byte(1), byte(2), byte(3)},
		}
		dataOut, err := toJSONTypes(dataIn)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{
			"nil":     nil,
			"int":     float64(12345.0),
			"int64":   float64(123456.0),
			"float32": float64(12.0),
			"float64": float64(23.0),
			"binary":  "AAECAw==",
		}, dataOut)
	}
}

func TestMessageToMemoryEvent(t *testing.T) {
	{
		// util.SchemaEventPayload
		payload := util.SchemaEventPayload{
			Payload: util.Payload{
				After: map[string]any{
					"foo":   "bar",
					"int64": int64(12341234),
				},
				Source: util.Source{
					TsMs:  12345000,
					Table: "table",
				},
				Operation: "r",
			},
		}
		message := lib.NewRawMessage("topic-suffix", map[string]any{"foo-pk": "bar-pk"}, &payload)
		evt, err := messageToMemoryEvent(message, &kafkalib.TopicConfig{})
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{
			"__artie_delete": false,
			"foo":            "bar",
			"int64":          float64(12341234),
		}, evt.Data)
	}
	{
		// mongo.SchemaEventPayload
		payload := mongo.SchemaEventPayload{
			Payload: mongo.Payload{
				After: ptr.ToString(`{"foo":"bar"}`),
				Source: mongo.Source{
					TsMs:       12345000,
					Collection: "collection",
				},
				Operation: "r",
			},
		}
		message := lib.NewRawMessage("topic-suffix", map[string]any{"foo-pk": "bar-pk"}, &payload)
		evt, err := messageToMemoryEvent(message, &kafkalib.TopicConfig{})
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"__artie_delete": true, "foo-pk": "bar-pk"}, evt.Data)
	}
}
