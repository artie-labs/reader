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
		assert.Equal(t, debezium.Bits, field.DebeziumType)
		assert.Equal(t, map[string]interface{}{"length": "0"}, field.Parameters)
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
		assert.Equal(t, map[string]interface{}{"length": "5"}, field.Parameters)
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
		// char max size - 5
		{
			// Invalid, length not matching
			converter := NewBitConverter(5)
			_, err := converter.Convert("101111")
			assert.ErrorContains(t, err, "bit converter failed: mismatched char max length")
		}
		{
			// Invalid, value contains non 0s and 1s
			converter := NewBitConverter(5)
			_, err := converter.Convert("1011a")
			assert.ErrorContains(t, err, "invalid binary string")
		}
		{
			// Valid
			converter := NewBitConverter(5)
			value, err := converter.Convert("10101")
			assert.NoError(t, err)
			assert.Equal(t, []byte{21}, value)
		}
		{
			// Valid #2
			converter := NewBitConverter(5)
			value, err := converter.Convert("10011")
			assert.NoError(t, err)
			assert.Equal(t, []byte{19}, value)
		}
	}
	{
		// char max size - 10
		converter := NewBitConverter(10)
		value, err := converter.Convert("1000000011")
		assert.NoError(t, err)
		assert.Equal(t, []byte{3, 2}, value)
	}
	{
		// char max size - 17
		converter := NewBitConverter(17)
		value, err := converter.Convert("10000000111111111")
		assert.NoError(t, err)
		assert.Equal(t, []byte{255, 1, 1}, value)
	}
	{
		// char max size - 24
		converter := NewBitConverter(24)
		value, err := converter.Convert("110110101111000111100101")
		assert.NoError(t, err)
		assert.Equal(t, []byte{229, 241, 218}, value)
	}
	{
		// char max size - 240 (which exceeds int64)
		converter := NewBitConverter(240)
		value, err := converter.Convert("110110101111000111100101110110101111000111100101110110101111000111100101110110101111000111100101110110101111000111100101110110101111000111100101110110101111000111100101110110101111000111100101110110101111000111100101110110101111000111100101")
		assert.NoError(t, err)
		assert.Equal(t, []byte{229, 241, 218, 229, 241, 218, 229, 241, 218, 229, 241, 218, 229, 241, 218, 229, 241, 218, 229, 241, 218, 229, 241, 218, 229, 241, 218, 229, 241, 218}, value)
	}
}
