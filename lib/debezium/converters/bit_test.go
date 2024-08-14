package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitConverter_Convert(t *testing.T) {
	converter := BitConverter{}
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
