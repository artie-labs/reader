package kafkalib

import (
	"testing"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib"
)

func TestNewMessage(t *testing.T) {
	pkMap := map[string]any{"key": "value"}
	rawMessage := lib.NewRawMessage(
		"topic-suffix",
		debezium.FieldsObject{},
		pkMap,
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
	)

	msg, err := buildKafkaMessageWrapper("topic-prefix", rawMessage)
	assert.NoError(t, err)
	assert.Equal(t, "topic-prefix.topic-suffix", msg.Topic)
	assert.Equal(t, `{"schema":{"type":"","fields":null,"optional":false,"field":""},"payload":{"key":"value"}}`, string(msg.MessageKey))
	assert.Equal(t, `{"schema":{"type":"","fields":null},"payload":{"before":null,"after":{"a":"b"},"source":{"connector":"","ts_ms":1000,"db":"","table":"table"},"op":"c"}}`, string(msg.MessageValue))

	// Parse this using JSON
	returnedPkMap, err := debezium.ParsePartitionKey(msg.MessageKey, kafkalib.JSONKeyFmt)
	assert.NoError(t, err)
	assert.Equal(t, pkMap, returnedPkMap)
}
