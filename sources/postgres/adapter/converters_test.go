package adapter

import (
	"math"
	"testing"
	"time"

	transferDbz "github.com/artie-labs/transfer/lib/debezium"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/debezium/converters"
)

func TestMoneyConverter_ToField(t *testing.T) {
	converter := MoneyConverter{}
	expected := transferDbz.Field{
		FieldName:    "col",
		DebeziumType: "org.apache.kafka.connect.data.Decimal",
		Parameters: map[string]any{
			"scale": "2",
		},
	}
	assert.Equal(t, expected, converter.ToField("col"))
}

func TestMoneyConverter_Convert(t *testing.T) {
	decodeValue := func(value any) string {
		stringValue, ok := value.(string)
		assert.True(t, ok)
		val, err := converters.NewDecimalConverter(moneyScale, nil).ToField("").DecodeDecimal(stringValue)
		assert.NoError(t, err)
		return val.String()
	}

	converter := MoneyConverter{}
	{
		// int
		converted, err := converter.Convert(1234)
		assert.NoError(t, err)
		assert.Equal(t, "AeII", converted)
		assert.Equal(t, "1234.00", decodeValue(converted))
	}
	{
		// float
		converted, err := converter.Convert(1234.56)
		assert.NoError(t, err)
		assert.Equal(t, "AeJA", converted)
		assert.Equal(t, "1234.56", decodeValue(converted))
	}
	{
		// string
		converted, err := converter.Convert("1234.56")
		assert.NoError(t, err)
		assert.Equal(t, "AeJA", converted)
		assert.Equal(t, "1234.56", decodeValue(converted))
	}
	{
		// string with $ and comma
		converted, err := converter.Convert("$1,234.567")
		assert.NoError(t, err)
		assert.Equal(t, "AeJA", converted)
		assert.Equal(t, "1234.56", decodeValue(converted))
	}
	{
		// string with $, comma, and no cents
		converted, err := converter.Convert("$1000,234")
		assert.NoError(t, err)
		assert.Equal(t, "BfY8aA==", converted)
		assert.Equal(t, "1000234.00", decodeValue(converted))
	}
}

func TestPgTimeConverter_ToField(t *testing.T) {
	converter := PgTimeConverter{}
	expected := transferDbz.Field{
		FieldName:    "col",
		Type:         "int32",
		DebeziumType: "io.debezium.time.Time",
	}
	assert.Equal(t, expected, converter.ToField("col"))
}

func TestPgTimeConverter_Convert(t *testing.T) {
	converter := PgTimeConverter{}
	{
		// Invalid type
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected pgtype.Time got int with value: 1234")
	}
	{
		// Invalid pgtype.Time
		value, err := converter.Convert(pgtype.Time{Valid: false})
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// Valid pgtype.Time - one microsecond
		value, err := converter.Convert(pgtype.Time{Valid: true, Microseconds: 1})
		assert.NoError(t, err)
		assert.Equal(t, int32(0), value)
	}
	{
		// Valid pgtype.Time - one millisecond
		value, err := converter.Convert(pgtype.Time{Valid: true, Microseconds: 1000})
		assert.NoError(t, err)
		assert.Equal(t, int32(1), value)
	}
	{
		// Valid pgtype.Time - one day
		value, err := converter.Convert(pgtype.Time{
			Valid:        true,
			Microseconds: int64((time.Duration(24) * time.Hour) / time.Microsecond),
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(86_400_000), value)
	}
	{
		// Valid pgtype.Time - negative overflow
		_, err := converter.Convert(pgtype.Time{
			Valid:        true,
			Microseconds: math.MinInt64,
		})
		assert.ErrorContains(t, err, "milliseconds overflows int32")
	}
	{
		// Valid pgtype.Time - positive overflow
		_, err := converter.Convert(pgtype.Time{
			Valid:        true,
			Microseconds: math.MaxInt64,
		})
		assert.ErrorContains(t, err, "milliseconds overflows int32")
	}
}

func TestPgIntervalConverter_ToField(t *testing.T) {
	converter := PgIntervalConverter{}
	expected := transferDbz.Field{
		FieldName:    "col",
		Type:         "int64",
		DebeziumType: "io.debezium.time.MicroDuration",
	}
	assert.Equal(t, expected, converter.ToField("col"))
}

func TestPgIntervalConverter_Convert(t *testing.T) {
	converter := PgIntervalConverter{}
	{
		// Invalid type
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected pgtype.Interval got int with value: 1234")
	}
	{
		// Invalid pgtype.Interval
		value, err := converter.Convert(pgtype.Interval{Valid: false})
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// Valid pgtype.Interval - one microsecond
		value, err := converter.Convert(pgtype.Interval{Valid: true, Microseconds: 1})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), value)
	}
	{
		// Valid pgtype.Interval - one day + two microseconds
		value, err := converter.Convert(pgtype.Interval{Valid: true, Days: 1, Microseconds: 2})
		assert.NoError(t, err)
		assert.Equal(t, int64(86_400_000_002), value)
	}
	{
		// Valid pgtype.Interval - one month + three microsecond
		value, err := converter.Convert(pgtype.Interval{Valid: true, Months: 1, Microseconds: 3})
		assert.NoError(t, err)
		assert.Equal(t, int64(2_629_800_000_003), value)
	}
	{
		// Valid pgtype.Interval - very large but no overflow
		value, err := converter.Convert(pgtype.Interval{Valid: true, Months: 292_000 * 12})
		assert.NoError(t, err)
		assert.Equal(t, int64(9_214_819_200_000_000_000), value)
	}
	{
		// Valid pgtype.Interval - overflow
		_, err := converter.Convert(pgtype.Interval{Valid: true, Months: 293_000 * 12})
		assert.ErrorContains(t, err, "positive microseconds are too large for an int64")
	}
	{
		// Valid pgtype.Interval - overflow
		_, err := converter.Convert(pgtype.Interval{Valid: true, Microseconds: math.MaxInt64, Days: 1})
		assert.ErrorContains(t, err, "positive microseconds are too large for an int64")
	}
	{
		// Valid pgtype.Interval - very large but no overflow
		value, err := converter.Convert(pgtype.Interval{Valid: true, Months: -292_000 * 12})
		assert.NoError(t, err)
		assert.Equal(t, int64(-9_214_819_200_000_000_000), value)
	}
	{
		// Valid pgtype.Interval - negative overflow
		_, err := converter.Convert(pgtype.Interval{Valid: true, Months: -293_000 * 12})
		assert.ErrorContains(t, err, "negative microseconds are too large for an int64")
	}
	{
		// Valid pgtype.Interval - positive overflow
		_, err := converter.Convert(pgtype.Interval{Valid: true, Microseconds: math.MinInt64, Days: -1})
		assert.ErrorContains(t, err, "negative microseconds are too large for an int64")
	}
}

func TestPgInetConverter_ToField(t *testing.T) {
	converter := PgInetConverter{}
	expected := transferDbz.Field{
		FieldName: "col",
		Type:      "string",
	}
	assert.Equal(t, expected, converter.ToField("col"))
}

func TestPgInetConverter_Convert(t *testing.T) {
	converter := PgInetConverter{}
	{
		// Invalid type
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected string got int with value: 1234")
	}
	{
		// inet - IPv4 without subnet
		value, err := converter.Convert("10.1.2.3")
		assert.NoError(t, err)
		assert.Equal(t, "10.1.2.3/32", value)
	}
	{
		// inet - IPv4 with subnet
		value, err := converter.Convert("10.1.2.3/8")
		assert.NoError(t, err)
		assert.Equal(t, "10.1.2.3/8", value)
	}
	{
		// inet - IPv6 without subnet
		value, err := converter.Convert("2001:4f8:3:ba:2e0:81ff:fe22:d1f1")
		assert.NoError(t, err)
		assert.Equal(t, "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", value)
	}
	{
		// inet - IPv6 with subnet
		value, err := converter.Convert("2001:4f8:3:ba:2e0:81ff:fe22:d1f1/64")
		assert.NoError(t, err)
		assert.Equal(t, "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/64", value)
	}
	{
		// inet - IPv6 with subnet
		value, err := converter.Convert("::ffff:1.2.3.0")
		assert.NoError(t, err)
		assert.Equal(t, "::ffff:1.2.3.0/128", value)
	}
}
