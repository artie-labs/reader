package s3lib

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAssertType(t *testing.T) {
	{
		// String to string
		val, err := assertType[string]("hello")
		assert.NoError(t, err)
		assert.Equal(t, "hello", val)
	}
	{
		// Int to string
		_, err := assertType[string](1)
		assert.ErrorContains(t, err, "expected type string, got int")
	}
	{
		// Boolean to boolean
		val, err := assertType[bool](true)
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}
	{
		// String to boolean
		_, err := assertType[bool]("true")
		assert.ErrorContains(t, err, "expected type bool, got string")
	}
}
