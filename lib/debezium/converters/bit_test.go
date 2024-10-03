package converters

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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
		assert.Equal(t, "hello", value.(string))
	}
}
