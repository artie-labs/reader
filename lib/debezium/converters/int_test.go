package converters

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAsInt16(t *testing.T) {
	{
		_, err := asInt16("not an int")
		assert.ErrorContains(t, err, "expected int/int16/int32/int64 got string with value: not an int")
	}
	{
		value, err := asInt16(int16(1234))
		assert.NoError(t, err)
		assert.Equal(t, int16(1234), value)
	}
	{
		// int32
		value, err := asInt16(int32(1234))
		assert.NoError(t, err)
		assert.Equal(t, int16(1234), value)
	}
	{
		// int32 - just large enough
		value, err := asInt16(int32(math.MaxInt16))
		assert.NoError(t, err)
		assert.Equal(t, int16(math.MaxInt16), value)
	}
	{
		// int32 - underflow
		_, err := asInt16(int32(math.MinInt16 - 1))
		assert.ErrorContains(t, err, "value is too large for int16")
	}
	{
		// int32 - overflow
		_, err := asInt16(int32(math.MaxInt16 + 1))
		assert.ErrorContains(t, err, "value is too large for int16")
	}
	{
		// int64
		value, err := asInt16(int64(1234))
		assert.NoError(t, err)
		assert.Equal(t, int16(1234), value)
	}
	{
		// int64 - just large enough
		value, err := asInt16(int64(math.MaxInt16))
		assert.NoError(t, err)
		assert.Equal(t, int16(math.MaxInt16), value)
	}
	{
		// int64 - underflow
		_, err := asInt16(int64(math.MinInt16 - 1))
		assert.ErrorContains(t, err, "value is too large for int16")
	}
	{
		// int64 - overflow
		_, err := asInt16(int64(math.MaxInt16 + 1))
		assert.ErrorContains(t, err, "value is too large for int16")
	}
	{
		// int
		value, err := asInt16(int(1234))
		assert.NoError(t, err)
		assert.Equal(t, int16(1234), value)
	}
	{
		// int - just large enough
		value, err := asInt16(int(math.MaxInt16))
		assert.NoError(t, err)
		assert.Equal(t, int16(math.MaxInt16), value)
	}
	{
		// int - underflow
		_, err := asInt16(int(math.MinInt16 - 1))
		assert.ErrorContains(t, err, "value is too large for int16")
	}
	{
		// int - overflow
		_, err := asInt16(int(math.MaxInt16 + 1))
		assert.ErrorContains(t, err, "value is too large for int16")
	}
}

func TestAsInt32(t *testing.T) {
	{
		_, err := asInt32("not an int")
		assert.ErrorContains(t, err, "expected int/int16/int32/int64 got string with value: not an int")
	}
	{
		// int16
		value, err := asInt32(int16(1234))
		assert.NoError(t, err)
		assert.Equal(t, int32(1234), value)
	}
	{
		// int32
		value, err := asInt32(int32(1234))
		assert.NoError(t, err)
		assert.Equal(t, int32(1234), value)
	}
	{
		// int64
		value, err := asInt32(int64(1234))
		assert.NoError(t, err)
		assert.Equal(t, int32(1234), value)
	}
	{
		// int64 - just large enough
		value, err := asInt32(int64(math.MaxInt32))
		assert.NoError(t, err)
		assert.Equal(t, int32(math.MaxInt32), value)
	}
	{
		// int64 - underflow
		_, err := asInt32(int64(math.MinInt32 - 1))
		assert.ErrorContains(t, err, "value is too large for int32")
	}
	{
		// int64 - overflow
		_, err := asInt32(int64(math.MaxInt32 + 1))
		assert.ErrorContains(t, err, "value is too large for int32")
	}
	{
		// int
		value, err := asInt32(int(1234))
		assert.NoError(t, err)
		assert.Equal(t, int32(1234), value)
	}
	{
		// int - just large enough
		value, err := asInt32(int(math.MaxInt32))
		assert.NoError(t, err)
		assert.Equal(t, int32(math.MaxInt32), value)
	}
	{
		// int - underflow
		_, err := asInt32(int(math.MinInt32 - 1))
		assert.ErrorContains(t, err, "value is too large for int32")
	}
	{
		// int - overflow
		_, err := asInt32(int(math.MaxInt32 + 1))
		assert.ErrorContains(t, err, "value is too large for int32")
	}
}

func TestAsInt64(t *testing.T) {
	{
		_, err := asInt64("not an int")
		assert.ErrorContains(t, err, "expected int/int16/int32/int64 got string with value: not an int")
	}
	{
		// int16
		value, err := asInt64(int16(1234))
		assert.NoError(t, err)
		assert.Equal(t, int64(1234), value)
	}
	{
		// int32
		value, err := asInt64(int32(1234))
		assert.NoError(t, err)
		assert.Equal(t, int64(1234), value)
	}
	{
		// int64
		value, err := asInt64(int64(1234))
		assert.NoError(t, err)
		assert.Equal(t, int64(1234), value)
	}
	{
		// int
		value, err := asInt64(int(1234))
		assert.NoError(t, err)
		assert.Equal(t, int64(1234), value)
	}
}
