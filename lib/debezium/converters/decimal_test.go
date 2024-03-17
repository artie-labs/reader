package converters

import (
	"encoding/base64"
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
		converted, err := converter.Convert("1.23")
		assert.NoError(t, err)
		bytes, ok := converted.([]byte)
		assert.True(t, ok)
		actualValue, err := converter.ToField("").DecodeDecimal(base64.StdEncoding.EncodeToString(bytes))
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
		assert.ErrorContains(t, err, "expected string got int with value: 1234")
	}
	{
		// Happy path
		converted, err := converter.Convert("12.34")
		assert.NoError(t, err)
		convertedMap, ok := converted.(VariableScaleDecimal)
		assert.True(t, ok)
		assert.Equal(t, VariableScaleDecimal{Scale: 2, Value: []byte{0x4, 0xd2}}, convertedMap)

		decimalConverter := NewDecimalConverter(int(convertedMap.Scale), nil).ToField("")
		actualValue, err := decimalConverter.DecodeDecimal(base64.StdEncoding.EncodeToString(convertedMap.Value))
		assert.NoError(t, err)
		assert.Equal(t, "12.34", fmt.Sprint(actualValue))
	}
}
