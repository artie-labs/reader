package converters

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeConverter_Convert(t *testing.T) {
	{
		// 1 nanosecond
		value, err := NewTimeConverter(time.Nanosecond).Convert(1)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), value)
	}
	{
		// 1 microsecond
		value, err := NewTimeConverter(time.Microsecond).Convert(1)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), value)
	}
	{
		// 1000 microseconds
		value, err := NewTimeConverter(time.Microsecond).Convert(1000)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), value)
	}
	{
		// 1 millisecond
		value, err := NewTimeConverter(time.Millisecond).Convert(1)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), value)
	}
	{
		// 1 second
		value, err := NewTimeConverter(time.Second).Convert(1)
		assert.NoError(t, err)
		assert.Equal(t, int32(1000), value)
	}
	{
		// 24 hours
		value, err := NewTimeConverter(time.Hour).Convert(24)
		assert.NoError(t, err)
		assert.Equal(t, int32(86_400_000), value)
	}
	{
		// 24 days
		value, err := NewTimeConverter(time.Hour).Convert(24 * 24)
		assert.NoError(t, err)
		assert.Equal(t, int32(2_073_600_000), value)
	}
	{
		// 25 days - overflows int32
		_, err := NewTimeConverter(time.Hour).Convert(24 * 25)
		assert.ErrorContains(t, err, "millisecond value is larger than MaxInt32: 2160000000")
	}
	{
		// -25 days - underflows int32
		_, err := NewTimeConverter(time.Hour).Convert(24 * -25)
		assert.ErrorContains(t, err, "millisecond value is smaller than MinInt32: -2160000000")
	}
}

func TestMicroTimeConverter_Convert(t *testing.T) {
	converter := MicroTimeConverter{}
	{
		// Invalid value
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected string got int with value: 1234")
	}
	{
		// Valid value - 0 seconds
		value, err := converter.Convert("00:00:00")
		assert.NoError(t, err)
		assert.Equal(t, int64(0), value)
	}
	{
		// Valid value - 1 second
		value, err := converter.Convert("00:00:01")
		assert.NoError(t, err)
		assert.Equal(t, int64(1000_000), value)
	}
	{
		// Valid value - 1 minute
		value, err := converter.Convert("00:01:00")
		assert.NoError(t, err)
		assert.Equal(t, int64(1000_000*60), value)
	}
	{
		// Valid value - 1 hour
		value, err := converter.Convert("01:00:00")
		assert.NoError(t, err)
		assert.Equal(t, int64(1000_000*60*60), value)
	}
}

func TestMicroDurationConverter_Convert(t *testing.T) {
	{
		// 1 nanosecond
		value, err := NewMicroDurationConverter(time.Nanosecond).Convert(1)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), value)
	}
	{
		// 1000 nanoseconds
		value, err := NewMicroDurationConverter(time.Nanosecond).Convert(1000)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), value)
	}
	{
		// a lot of nanoseconds
		value, err := NewMicroDurationConverter(time.Nanosecond).Convert(math.MaxInt64)
		assert.NoError(t, err)
		assert.Equal(t, math.MaxInt64/int64(1000), value)
	}
	{
		// a lot of nanoseconds - overflow
		_, err := NewMicroDurationConverter(time.Nanosecond * 2).Convert(math.MaxInt64)
		assert.ErrorContains(t, err, "microsecond value is larger than MaxInt64")
	}
	{
		// a lot of negative nanoseconds - underflow
		_, err := NewMicroDurationConverter(time.Nanosecond * 2).Convert(math.MinInt64)
		assert.ErrorContains(t, err, "microsecond value is smaller than MinInt64")
	}
	{
		// 1 microsecond
		value, err := NewMicroDurationConverter(time.Microsecond).Convert(1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), value)
	}
	{
		// 1 millisecond
		value, err := NewMicroDurationConverter(time.Millisecond).Convert(1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1000), value)
	}
	{
		// 1 second
		value, err := NewMicroDurationConverter(time.Second).Convert(1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1_000_000), value)
	}
	{
		// 1000 years
		value, err := NewMicroDurationConverter(time.Hour).Convert(24 * 365 * 1000)
		assert.NoError(t, err)
		assert.Equal(t, int64(31_536_000_000_000_000), value)
	}
	{
		// 100,000 years (as hours)
		value, err := NewMicroDurationConverter(time.Hour).Convert(24 * 365 * 100_000)
		assert.NoError(t, err)
		assert.Equal(t, int64(3_153_600_000_000_000_000), value)
	}
	{
		// 100,000 years (as seconds)
		value, err := NewMicroDurationConverter(time.Second).Convert(60 * 60 * 24 * 365 * 100_000)
		assert.NoError(t, err)
		assert.Equal(t, int64(3_153_600_000_000_000_000), value)
	}
	{
		// 292,471 years
		value, err := NewMicroDurationConverter(time.Second).Convert(60 * 60 * 24 * 365 * 292_471)
		assert.NoError(t, err)
		assert.Equal(t, int64(9_223_365_456_000_000_000), value)
	}
	{
		// 292,472 years - overflows
		_, err := NewMicroDurationConverter(time.Second).Convert(60 * 60 * 24 * 365 * 292_472)
		assert.ErrorContains(t, err, "microsecond value is larger than MaxInt64")
	}
	{
		// -292,471 years
		value, err := NewMicroDurationConverter(time.Second).Convert(60 * 60 * 24 * 365 * -292_471)
		assert.NoError(t, err)
		assert.Equal(t, int64(-9_223_365_456_000_000_000), value)
	}
	{
		// -292,472 years - underflows
		_, err := NewMicroDurationConverter(time.Second).Convert(60 * 60 * 24 * 365 * -292_472)
		assert.ErrorContains(t, err, "microsecond value is smaller than MinInt64")
	}
}

func TestDateConverter_Convert(t *testing.T) {
	converter := DateConverter{}
	{
		// Invalid value
		_, err := converter.Convert(12345)
		assert.ErrorContains(t, err, "expected string/time.Time got int with value: 12345")
	}
	{
		// string - 0000-00-00
		value, err := converter.Convert("0000-00-00")
		assert.NoError(t, err)
		assert.Equal(t, nil, value)
	}
	{
		// string - malformed
		_, err := converter.Convert("aaaa-bb-cc")
		assert.ErrorContains(t, err, `failed to convert to date: parsing time "aaaa-bb-cc" as "2006-01-02"`)
	}
	{
		// string - 2023-05-03
		value, err := converter.Convert("2023-05-03")
		assert.NoError(t, err)
		assert.Equal(t, int32(19480), value)
	}
	{
		// time.Time - Unix epoch
		days, err := converter.Convert(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(0), days)
	}
	{
		// time.Time - Unix epoch + 1 day
		days, err := converter.Convert(time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(1), days)
	}
	{
		// time.Time - Unix epoch + 1 year
		value, err := converter.Convert(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(365), value)
	}
	{
		// time.Time - 2003
		days, err := converter.Convert(time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(19480), days)
	}
	{
		// time.Time - 1969
		days, err := converter.Convert(time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(-365), days)
	}
	{
		// time.Time - Year 9999
		days, err := converter.Convert(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(2_932_532), days)
	}
	{
		// time.Time - Year 10_000
		days, err := converter.Convert(time.Date(10_000, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(2_932_897), days)
	}
	{
		// time.Time - Year 0
		days, err := converter.Convert(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(-719_528), days)
	}
	{
		// time.Time - Year -1
		days, err := converter.Convert(time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(-719_893), days)
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
