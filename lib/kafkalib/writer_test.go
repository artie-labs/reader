package kafkalib

import (
	"testing"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	rawMessage := lib.NewRawMessage(
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
	)

	msg, err := buildKafkaMessage("topic-prefix", rawMessage)
	assert.NoError(t, err)
	assert.Equal(t, "topic-prefix.topic-suffix", msg.Topic)
	assert.Equal(t, `{"key":"value"}`, string(msg.Key))
	assert.Equal(t, `{"schema":{"type":"","fields":null},"payload":{"before":null,"after":{"a":"b"},"source":{"connector":"","ts_ms":1000,"db":"","schema":"","table":"table"},"op":"c"}}`, string(msg.Value))
}
