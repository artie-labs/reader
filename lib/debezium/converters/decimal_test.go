package converters

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

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
		converter := NewDecimalConverter(2, ptr.ToInt(3))
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
		actualValue, err := converter.ToField("").DecodeDecimal(bytes)
		assert.NoError(t, err)
		assert.Equal(t, "1.23", fmt.Sprint(actualValue))
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
		actualValue, err := converter.ToField("").DecodeDebeziumVariableDecimal(converted)
		assert.NoError(t, err)
		assert.Equal(t, "12.34", actualValue.String())
	}
}
