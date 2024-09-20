package converters

import (
	"math"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func parseUsingTransfer(converter ValueConverter, value int64) (*ext.ExtendedTime, error) {
	parsedValue, err := converter.ToField("foo").ParseValue(value)
	if err != nil {
		return nil, err
	}

	return typing.AssertType[*ext.ExtendedTime](parsedValue)
}

func TestTimeConverter_Convert(t *testing.T) {
	converter := TimeConverter{}
	{
		// Invalid value
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected time.Time got int with value: 1234")
	}
	{
		// Invalid value (string)
		_, err := converter.Convert("1234")
		assert.ErrorContains(t, err, "expected time.Time got string with value: 1234")
	}
	{
		// Valid value
		value, err := converter.Convert(time.Date(2023, 5, 3, 12, 34, 56, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int32(45296000), value)
	}
}

func TestMicroTimeConverter_Convert(t *testing.T) {
	converter := MicroTimeConverter{}
	{
		// Invalid value
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected string/time.Time got int with value: 1234")
	}
	{
		// Valid value - 0 seconds (time.Time)
		value, err := converter.Convert(time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int64(0), value)
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
		assert.Equal(t, int64(1_000_000), value)
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
	{
		// Transfer parsing
		value, err := converter.Convert("01:02:03")
		assert.NoError(t, err)
		transferValue, err := parseUsingTransfer(converter, value.(int64))
		assert.NoError(t, err)
		assert.Equal(t, time.Date(1970, time.January, 1, 1, 2, 3, 0, time.UTC), transferValue.GetTime())
		assert.Equal(t, ext.TimeKindType, transferValue.GetNestedKind().Type)
	}
}

func TestNanoTimeConverter_Convert(t *testing.T) {
	converter := NanoTimeConverter{}
	{
		// Invalid value
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected time.Time got int with value: 1234")
	}
	{
		// Valid value - 0 seconds (time.Time)
		value, err := converter.Convert(time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int64(0), value)
	}
	{
		// Valid value - 1 hour (time.Time)
		value, err := converter.Convert(time.Date(2023, 5, 3, 1, 0, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int64(3_600_000_000_000), value)
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
	{
		// Transfer parsing
		value, err := converter.Convert("2023-05-03")
		assert.NoError(t, err)
		transferValue, err := parseUsingTransfer(converter, int64(value.(int32)))
		assert.NoError(t, err)
		assert.Equal(t, time.Date(2023, time.May, 3, 0, 0, 0, 0, time.UTC), transferValue.GetTime())
		assert.Equal(t, ext.DateKindType, transferValue.GetNestedKind().Type)
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
		// Valid value 2024-05-16 12:34:56.000
		value, err := converter.Convert(time.Date(2024, 5, 16, 12, 34, 56, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int64(1715862896000), value)
	}
}

func TestMicroTimestampConverter_Convert(t *testing.T) {
	converter := MicroTimestampConverter{}
	{
		// Invalid type
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected time.Time got int with value: 1234")
	}
	{
		// Date > 9999
		value, err := converter.Convert(time.Date(9_9999, 2, 3, 4, 5, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, value, int64(3093499310700000000))
	}
	{
		// Date < 0
		value, err := converter.Convert(time.Date(-1, 2, 3, 4, 5, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int64(-62195889300000000), value)
	}
	{
		// time.Time
		value, err := converter.Convert(time.Date(2001, 2, 3, 4, 5, 0, 0, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int64(981173100000000), value)
	}
	{
		// Transfer parsing
		timeValue := time.Date(2001, 2, 3, 4, 5, 0, 0, time.UTC)
		value, err := converter.Convert(timeValue)
		assert.NoError(t, err)
		transferValue, err := parseUsingTransfer(converter, value.(int64))
		assert.NoError(t, err)
		assert.Equal(t, timeValue, transferValue.GetTime())
		assert.Equal(t, ext.TimestampTzKindType, transferValue.GetNestedKind().Type)
	}
}

func TestNanoTimestampConverter_Convert(t *testing.T) {
	converter := NanoTimestampConverter{}
	{
		// Invalid type
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected time.Time got int with value: 1234")
	}
	{
		// Valid 2024-05-16 12:34:56.1234567
		value, err := converter.Convert(time.Date(2024, 5, 16, 12, 34, 56, 123456700, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, int64(1715862896123456000), value)
	}
}

func TestZonedTimestampConverter_Convert(t *testing.T) {
	converter := ZonedTimestampConverter{}
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
		_ts := time.Date(2001, 2, 3, 4, 5, 0, 0, time.UTC)
		value, err := converter.Convert(_ts)
		assert.NoError(t, err)
		assert.Equal(t, "2001-02-03T04:05:00Z", value)

		// Check Transfer to ensure no precision loss
		ts, err := converters.DateTimeWithTimezone{}.Convert(value)
		assert.NoError(t, err)
		assert.Equal(t, _ts, ts.(*ext.ExtendedTime).GetTime())
	}
	{
		// time.Time (ms)
		_ts := time.Date(2001, 2, 3, 4, 5, 1, 900000, time.UTC)
		value, err := converter.Convert(_ts)
		assert.NoError(t, err)
		assert.Equal(t, "2001-02-03T04:05:01.0009Z", value)

		// Check Transfer to ensure no precision loss
		ts, err := converters.DateTimeWithTimezone{}.Convert(value)
		assert.NoError(t, err)
		assert.Equal(t, _ts, ts.(*ext.ExtendedTime).GetTime())
	}
	{
		// time.Time (microseconds)
		_ts := time.Date(2001, 2, 3, 4, 5, 1, 909000, time.UTC)
		value, err := converter.Convert(_ts)
		assert.NoError(t, err)
		assert.Equal(t, "2001-02-03T04:05:01.000909Z", value)

		// Check Transfer to ensure no precision loss
		ts, err := converters.DateTimeWithTimezone{}.Convert(value)
		assert.NoError(t, err)
		assert.Equal(t, _ts, ts.(*ext.ExtendedTime).GetTime())
	}
	{
		// Different timezone
		_ts := time.Date(2001, 2, 3, 4, 5, 0, 0, time.FixedZone("CET", 1*60*60))
		value, err := converter.Convert(_ts)
		assert.NoError(t, err)
		assert.Equal(t, "2001-02-03T03:05:00Z", value)

		// Check Transfer to ensure no precision loss
		ts, err := converters.DateTimeWithTimezone{}.Convert(value)
		assert.NoError(t, err)
		assert.Equal(t, _ts.UTC(), ts.(*ext.ExtendedTime).GetTime())
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
		assert.ErrorContains(t, err, "value overflows int32")
	}
}
