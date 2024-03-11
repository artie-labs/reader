package adapter

import (
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
}
