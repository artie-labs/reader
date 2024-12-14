package converters

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArrayConverter_ToField(t *testing.T) {
	{
		// String
		converter := NewArrayConverter("string")
		field := converter.ToField("name")
		assert.Equal(t, debezium.Field{
			FieldName:     "name",
			Type:          debezium.Array,
			ItemsMetadata: nil,
		}, field)
	}
	{
		// json[]
		converter := NewArrayConverter("json")
		field := converter.ToField("name")
		assert.Equal(t, debezium.Field{
			FieldName: "name",
			Type:      debezium.Array,
			ItemsMetadata: &debezium.Item{
				DebeziumType: debezium.JSON,
			},
		}, field)
	}
	{
		// jsonb[]
		converter := NewArrayConverter("jsonb")
		field := converter.ToField("name")
		assert.Equal(t, debezium.Field{
			FieldName: "name",
			Type:      debezium.Array,
			ItemsMetadata: &debezium.Item{
				DebeziumType: debezium.JSON,
			},
		}, field)
	}
}
