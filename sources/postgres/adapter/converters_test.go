package adapter

import (
	"testing"

	"github.com/artie-labs/reader/lib/debezium/converters"
	transferDbz "github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
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
