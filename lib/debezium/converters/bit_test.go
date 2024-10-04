package converters

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBitConverter_ToField(t *testing.T) {
	{
		// char size not specified
		field := NewBitConverter(0).ToField("foo")
		assert.Equal(t, "foo", field.FieldName)
		assert.Equal(t, "bytes", string(field.Type))
		assert.Equal(t, map[string]interface{}{"length": 0}, field.Parameters)
		assert.Equal(t, debezium.Bits, field.DebeziumType)
	}
	{
		// char max size 1
		field := NewBitConverter(1).ToField("foo")
		assert.Equal(t, "foo", field.FieldName)
		assert.Equal(t, "boolean", string(field.Type))
	}
	{
		// char max size 5
		field := NewBitConverter(5).ToField("foo")
		assert.Equal(t, "foo", field.FieldName)
		assert.Equal(t, "bytes", string(field.Type))
		assert.Equal(t, debezium.Bits, field.DebeziumType)
	}
}

func TestBitConverter_Convert(t *testing.T) {
	{
		// char size not specified
		_, err := BitConverter{}.Convert("foo")
		assert.ErrorContains(t, err, "bit converter failed: invalid char max length")
	}
	{
		// char max size 1
		converter := NewBitConverter(1)
		{
			// Invalid value - wrong type
			_, err := converter.Convert(1234)
			assert.ErrorContains(t, err, "expected type string, got int")
		}
		{
			// Valid value - 0
			value, err := converter.Convert("0")
			assert.NoError(t, err)
			assert.Equal(t, false, value)
		}
		{
			// Valid value - 1
			value, err := converter.Convert("1")
			assert.NoError(t, err)
			assert.Equal(t, true, value)
		}
		{
			// Invalid value - 2
			_, err := converter.Convert("2")
			assert.ErrorContains(t, err, `string value "2" is not in ["0", "1"]`)
		}
	}
	{
		// char max size 5
		converter := NewBitConverter(5)
		value, err := converter.Convert("hello")
		assert.NoError(t, err)
		assert.Equal(t, []byte("hello"), value)
	}
}
