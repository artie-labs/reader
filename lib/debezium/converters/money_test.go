package converters

import (
	transferDbz "github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMoney_Scale(t *testing.T) {
	{
		// Not specified
		converter := MoneyConverter{}
		assert.Equal(t, defaultScale, converter.Scale())
	}
	{
		// Specified
		converter := MoneyConverter{
			ScaleOverride: ptr.ToInt(3),
		}
		assert.Equal(t, 3, converter.Scale())
	}
}

func TestMoneyConverter_ToField(t *testing.T) {
	converter := MoneyConverter{}
	expected := transferDbz.Field{
		FieldName:    "col",
		Type:         "bytes",
		DebeziumType: "org.apache.kafka.connect.data.Decimal",
		Parameters: map[string]any{
			"scale": "2",
		},
	}
	assert.Equal(t, expected, converter.ToField("col"))
}

func TestMoneyConverter_Convert(t *testing.T) {
	decimalField := NewDecimalConverter(defaultScale, nil).ToField("")
	decodeValue := func(value any) string {
		bytes, ok := value.([]byte)
		assert.True(t, ok)
		val, err := decimalField.DecodeDecimal(bytes)
		assert.NoError(t, err)
		return val.String()
	}
	{
		// Converter where mutateString is true
		converter := MoneyConverter{
			StripCommas:    true,
			CurrencySymbol: "$",
		}
		{
			// string
			converted, err := converter.Convert("1234.56")
			assert.NoError(t, err)
			assert.Equal(t, []byte{0x1, 0xe2, 0x40}, converted)
			assert.Equal(t, "1234.56", decodeValue(converted))
		}
		{
			// string with $ and comma
			converted, err := converter.Convert("$1,234.567")
			assert.NoError(t, err)
			assert.Equal(t, []byte{0x1, 0xe2, 0x40}, converted)
			assert.Equal(t, "1234.56", decodeValue(converted))
		}
		{
			// string with $, comma, and no cents
			converted, err := converter.Convert("$1000,234")
			assert.NoError(t, err)
			assert.Equal(t, []byte{0x5, 0xf6, 0x3c, 0x68}, converted)
			assert.Equal(t, "1000234.00", decodeValue(converted))
		}
		{
			// Malformed string - empty string.
			_, err := converter.Convert("")
			assert.ErrorContains(t, err, "unable to use '' as a floating-point number")
		}
		{
			// Malformed string - not a floating-point.
			_, err := converter.Convert("malformed")
			assert.ErrorContains(t, err, "unable to use 'malformed' as a floating-point number")
		}
	}
	{
		// Converter where mutateString is false
		converter := MoneyConverter{}
		{
			// int
			converted, err := converter.Convert("1234.567")
			assert.NoError(t, err)
			assert.Equal(t, []byte{0x1, 0xe2, 0x40}, converted)
			assert.Equal(t, "1234.56", decodeValue(converted))
		}
	}

}