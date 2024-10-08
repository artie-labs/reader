package converters

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecimalWithScale(t *testing.T) {
	mustEncodeAndDecodeDecimal := func(value string, scale int32) string {
		bytes, err := encodeDecimalWithScale(numbers.MustParseDecimal(value), scale)
		assert.NoError(t, err)
		return converters.DecodeDecimal(bytes, scale).String()
	}

	mustReturnError := func(value string, scale int32) error {
		_, err := encodeDecimalWithScale(numbers.MustParseDecimal(value), scale)
		assert.Error(t, err)
		return err
	}

	// Whole numbers:
	for i := range 100_000 {
		strValue := fmt.Sprint(i)
		assert.Equal(t, strValue, mustEncodeAndDecodeDecimal(strValue, 0))
		if i != 0 {
			strValue := "-" + strValue
			assert.Equal(t, strValue, mustEncodeAndDecodeDecimal(strValue, 0))
		}
	}

	// Scale of 15 that is equal to the amount of decimal places in the value:
	assert.Equal(t, "145.183000000000000", mustEncodeAndDecodeDecimal("145.183000000000000", 15))
	assert.Equal(t, "-145.183000000000000", mustEncodeAndDecodeDecimal("-145.183000000000000", 15))
	// If scale is smaller than the amount of decimal places then an error should be returned:
	assert.ErrorContains(t, mustReturnError("145.183000000000000", 14), "value scale (15) is different from schema scale (14)")
	// If scale is larger than the amount of decimal places then an error should be returned:
	assert.ErrorContains(t, mustReturnError("-145.183000000000005", 16), "value scale (15) is different from schema scale (16)")

	assert.Equal(t, "-9063701308.217222135", mustEncodeAndDecodeDecimal("-9063701308.217222135", 9))
	assert.Equal(t, "-74961544796695.89960242", mustEncodeAndDecodeDecimal("-74961544796695.89960242", 8))

	// Values that are not finite:
	assert.ErrorContains(t, mustReturnError("NaN", 5), "decimal (NaN) is not finite")
	assert.ErrorContains(t, mustReturnError("Infinity", 5), "decimal (Infinity) is not finite")
	assert.ErrorContains(t, mustReturnError("-Infinity", 5), "decimal (-Infinity) is not finite")

	testCases := []struct {
		name  string
		value string
		scale int32
	}{
		{
			name:  "0 scale",
			value: "5",
		},
		{
			name:  "2 scale",
			value: "23131319.99",
			scale: 2,
		},
		{
			name:  "5 scale",
			value: "9.12345",
			scale: 5,
		},
		{
			name:  "negative number",
			value: "-105.2813669",
			scale: 7,
		},
		// Longitude #1
		{
			name:  "long 1",
			value: "-75.765611",
			scale: 6,
		},
		// Latitude #1
		{
			name:  "lat",
			value: "40.0335495",
			scale: 7,
		},
		// Long #2
		{
			name:  "long 2",
			value: "-119.65575",
			scale: 5,
		},
		{
			name:  "lat 2",
			value: "36.3303",
			scale: 4,
		},
		{
			name:  "long 3",
			value: "-81.76254098",
			scale: 8,
		},
		{
			name:  "amount",
			value: "6408.355",
			scale: 3,
		},
		{
			name:  "total",
			value: "1.05",
			scale: 2,
		},
		{
			name:  "negative number: 2^16 - 255",
			value: "-65281",
			scale: 0,
		},
		{
			name:  "negative number: 2^16 - 1",
			value: "-65535",
			scale: 0,
		},
		{
			name:  "number with a scale of 15",
			value: "0.000022998904125",
			scale: 15,
		},
		{
			name:  "number with a scale of 15",
			value: "145.183000000000000",
			scale: 15,
		},
	}

	for _, testCase := range testCases {
		actual := mustEncodeAndDecodeDecimal(testCase.value, testCase.scale)
		assert.Equal(t, testCase.value, actual, testCase.name)
	}
}

func TestDecimalConverter_ToField(t *testing.T) {
	{
		// Without precision
		converter := NewDecimalConverter(2, nil)
		expected := debezium.Field{
			Type:         "bytes",
			FieldName:    "col",
			DebeziumType: "org.apache.kafka.connect.data.Decimal",
			Parameters: map[string]any{
				"scale": "2",
			},
		}
		assert.Equal(t, expected, converter.ToField("col"))
	}
	{
		// With precision
		converter := NewDecimalConverter(2, typing.ToPtr(3))
		expected := debezium.Field{
			Type:         "bytes",
			FieldName:    "col",
			DebeziumType: "org.apache.kafka.connect.data.Decimal",
			Parameters: map[string]any{
				"connect.decimal.precision": "3",
				"scale":                     "2",
			},
		}
		assert.Equal(t, expected, converter.ToField("col"))
	}
}

func TestDecimalConverter_Convert(t *testing.T) {
	converter := NewDecimalConverter(2, nil)
	{
		// Malformed value - empty string.
		_, err := converter.Convert("")
		assert.ErrorContains(t, err, `unable to use "" as a decimal: parse mantissa:`)
	}
	{
		// Malformed value - not a floating-point.
		_, err := converter.Convert("11qwerty00")
		assert.ErrorContains(t, err, `unable to use "11qwerty00" as a decimal: parse exponent:`)
	}
	{
		// Happy path.
		converted, err := converter.Convert("1.23")
		assert.NoError(t, err)
		bytes, ok := converted.([]byte)
		assert.True(t, ok)

		actualValue, err := converter.ToField("").ParseValue(bytes)
		assert.NoError(t, err)
		assert.Equal(t, "1.23", actualValue.(*decimal.Decimal).String())
	}
	{
		// NaN:
		converted, err := converter.Convert("NaN")
		assert.NoError(t, err)
		assert.Nil(t, converted)
	}
}

func TestVariableNumericConverter_ToField(t *testing.T) {
	converter := VariableNumericConverter{}
	expected := debezium.Field{
		FieldName:    "col",
		Type:         "struct",
		DebeziumType: "io.debezium.data.VariableScaleDecimal",
	}
	assert.Equal(t, expected, converter.ToField("col"))
}

func TestVariableNumericConverter_Convert(t *testing.T) {
	converter := VariableNumericConverter{}
	{
		// Wrong type
		_, err := converter.Convert(1234)
		assert.ErrorContains(t, err, "expected type string, got int")
	}
	{
		// Malformed value - empty string.
		_, err := converter.Convert("")
		assert.ErrorContains(t, err, `unable to use "" as a decimal: parse mantissa:`)
	}
	{
		// Malformed value - not a floating point.
		_, err := converter.Convert("malformed")
		assert.ErrorContains(t, err, `unable to use "malformed" as a decimal: parse exponent`)
	}
	{
		// Happy path
		converted, err := converter.Convert("12.34")
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"scale": int32(2), "value": []byte{0x4, 0xd2}}, converted)

		actualValue, err := converters.NewVariableDecimal().Convert(converted)
		assert.NoError(t, err)
		assert.Equal(t, "12.34", actualValue.(*decimal.Decimal).String())
	}
	{
		// NaN:
		converted, err := converter.Convert("NaN")
		assert.NoError(t, err)
		assert.Nil(t, converted)
	}
}
