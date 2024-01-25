package kafkalib

import (
	"testing"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	payload := util.SchemaEventPayload{
		Payload: util.Payload{
			After: map[string]interface{}{"a": "b"},
			Source: util.Source{
				TsMs:  1000,
				Table: "table",
			},
			Operation: "c",
		},
	}

	msg, err := NewMessage("topic", map[string]interface{}{"key": "value"}, payload)
	assert.NoError(t, err)
	assert.Equal(t, "topic", msg.Topic)
	assert.Equal(t, `{"key":"value"}`, string(msg.Key))
	assert.Equal(t, `{"schema":{"type":"","fields":null},"payload":{"before":null,"after":{"a":"b"},"source":{"connector":"","ts_ms":1000,"db":"","schema":"","table":"table"},"op":"c"}}`, string(msg.Value))
}
