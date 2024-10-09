package converters

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBitVaryingConverter_ToField(t *testing.T) {
	{
		// char size not specified
		field := NewBitVaryingConverter(0).ToField("foo")
		assert.Equal(t, "foo", field.FieldName)
		assert.Equal(t, "bytes", string(field.Type))
		assert.Equal(t, debezium.Bits, field.DebeziumType)
		assert.Nil(t, field.Parameters)
	}
	{
		// char max size 1
		field := NewBitVaryingConverter(1).ToField("foo")
		assert.Equal(t, "foo", field.FieldName)
		assert.Equal(t, "bytes", string(field.Type))
		assert.Nil(t, field.Parameters)
	}
	{
		// char max size 5
		field := NewBitVaryingConverter(5).ToField("foo")
		assert.Equal(t, "foo", field.FieldName)
		assert.Equal(t, "bytes", string(field.Type))
		assert.Equal(t, debezium.Bits, field.DebeziumType)
		assert.Nil(t, field.Parameters)
	}
}

func TestBitVaryingConverter_Convert(t *testing.T) {
	{
		// char size not specified
		_, err := BitVaryingConverter{}.Convert("foo")
		assert.ErrorContains(t, err, `invalid binary string "foo": contains non-binary characters`)
	}
	{
		// char max size 1
		converter := NewBitVaryingConverter(1)
		{
			// Invalid value - wrong type
			_, err := converter.Convert(1234)
			assert.ErrorContains(t, err, "expected type string, got int")
		}
		{
			// Valid value - 0
			value, err := converter.Convert("0")
			assert.NoError(t, err)
			assert.Equal(t, []byte{}, value)
		}
		{
			// Valid value - 1
			value, err := converter.Convert("1")
			assert.NoError(t, err)
			assert.Equal(t, []byte{1}, value)
		}
		{
			// Invalid value - 2
			_, err := converter.Convert("2")
			assert.ErrorContains(t, err, `invalid binary string "2": contains non-binary characters`)
		}
	}
	{
		// char max size - 5
		{
			// Length not matching, but it's fine.
			converter := NewBitVaryingConverter(8)
			value, err := converter.Convert("101111")
			assert.NoError(t, err)
			assert.Equal(t, []byte{47}, value)
		}
		{
			// Invalid, value contains non 0s and 1s
			converter := NewBitVaryingConverter(5)
			_, err := converter.Convert("1011a")
			assert.ErrorContains(t, err, "invalid binary string")
		}
		{
			// Valid
			converter := NewBitVaryingConverter(5)
			value, err := converter.Convert("10101")
			assert.NoError(t, err)
			assert.Equal(t, []byte{21}, value)
		}
		{
			// Valid #2
			converter := NewBitVaryingConverter(5)
			value, err := converter.Convert("10011")
			assert.NoError(t, err)
			assert.Equal(t, []byte{19}, value)
		}
	}
}
