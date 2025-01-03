package kafkalib

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessagePartitionKey(t *testing.T) {
	msg := NewMessage("suffix", debezium.FieldsObject{}, nil, nil)
	assert.Equal(t, "suffix", msg.Topic(""), "no prefix")
	assert.Equal(t, "prefix.suffix", msg.Topic("prefix"), "with prefix")
}
