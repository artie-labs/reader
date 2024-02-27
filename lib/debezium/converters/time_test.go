package converters

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDateConverter_Convert(t *testing.T) {
	converter := DateConverter{}
	{
		// Invalid value
		_, err := converter.Convert("string value")
		assert.ErrorContains(t, err, "object is not a time.Time object")
	}
	{
		// time.Time
		value, err := converter.Convert(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, value, 365)
	}
}

func TestTimestampConverter_Convert(t *testing.T) {
	converter := TimestampConverter{}
	{
		// Invalid type
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected time.Time got int with value: 1234")
	}
	{
		// Date > 9999
		value, err := converter.Convert(time.Date(9_9999, 2, 3, 4, 5, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// Date < 0
		value, err := converter.Convert(time.Date(-1, 2, 3, 4, 5, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// time.Time
		value, err := converter.Convert(time.Date(2001, 2, 3, 4, 5, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, time.Date(2001, 2, 3, 4, 5, 0, 0, time.UTC), value)
	}
}

func TestYearConverter_Convert(t *testing.T) {
	converter := YearConverter{}
	{
		// Invalid type
		_, err := converter.Convert("asdf")
		assert.ErrorContains(t, err, "expected int/int16/int32/int64 got string with value: asdf")
	}
	{
		// int16
		value, err := converter.Convert(int16(1234))
		assert.NoError(t, err)
		assert.Equal(t, int32(1234), value)
	}
	{
		// int32
		value, err := converter.Convert(int32(1234))
		assert.NoError(t, err)
		assert.Equal(t, int32(1234), value)
	}
	{
		// int64
		value, err := converter.Convert(int64(1234))
		assert.NoError(t, err)
		assert.Equal(t, int32(1234), value)
	}
	{
		// int64 - too big
		_, err := converter.Convert(int64(math.MaxInt32 + 1))
		assert.ErrorContains(t, err, "value is too large for int32")
	}
}
