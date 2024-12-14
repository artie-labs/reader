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

func TestArrayConverter(t *testing.T) {
	{
		// Array of strings
		list := []any{"a", "b", "c"}
		converter := NewArrayConverter("string")
		converted, err := converter.Convert(list)
		assert.NoError(t, err)

		returnedValue, err := converter.ToField("name").ParseValue(converted)
		assert.NoError(t, err)
		assert.Equal(t, list, returnedValue)
	}
	{
		// Array of jsonb[]
		{
			// Invalid - item type is JSON objects
			list := []any{map[string]any{"a": "b"}, map[string]any{"c": "d"}}
			converter := NewArrayConverter("jsonb")
			converted, err := converter.Convert(list)
			assert.NoError(t, err)

			_, err = converter.ToField("name").ParseValue(converted)
			assert.ErrorContains(t, err, `expected string, got map[string]interface {}, value 'map[a:b]'`)
		}
		{
			// Valid - item type is JSON strings
		}
	}
}
