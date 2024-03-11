package adapter

import (
	"math"
	"testing"

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
		// Valid pgtype.Interval - very large but no underflow
		value, err := converter.Convert(pgtype.Interval{Valid: true, Months: -292_000 * 12})
		assert.NoError(t, err)
		assert.Equal(t, int64(-9_214_819_200_000_000_000), value)
	}
	{
		// Valid pgtype.Interval - underflow
		_, err := converter.Convert(pgtype.Interval{Valid: true, Months: -293_000 * 12})
		assert.ErrorContains(t, err, "negative microseconds are too large for an int64")
	}
	{
		// Valid pgtype.Interval - underflow
		_, err := converter.Convert(pgtype.Interval{Valid: true, Microseconds: math.MinInt64, Days: -1})
		assert.ErrorContains(t, err, "negative microseconds are too large for an int64")
	}
}
