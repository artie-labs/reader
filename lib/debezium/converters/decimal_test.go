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
		converted, err := converter.Convert("1.23")
		assert.NoError(t, err)
		bytes, ok := converted.([]byte)
		assert.True(t, ok)
		actualValue, err := converter.ToField("").DecodeDecimal(bytes)
		assert.NoError(t, err)
		assert.Equal(t, "1.23", fmt.Sprint(actualValue))
	}
}

func TestGetScale(t *testing.T) {
	type _testCase struct {
		name          string
		value         string
		expectedScale int
	}

	testCases := []_testCase{
		{
			name:          "0 scale",
			value:         "5",
			expectedScale: 0,
		},
		{
			name:          "2 scale",
			value:         "9.99",
			expectedScale: 2,
		},
		{
			name:          "5 scale",
			value:         "9.12345",
			expectedScale: 5,
		},
	}

	for _, testCase := range testCases {
		actualScale := getScale(testCase.value)
		assert.Equal(t, testCase.expectedScale, actualScale, testCase.name)
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
		assert.Equal(t, map[string]any{"scale": int32(2), "value": []byte{0x4, 0xd2}}, converted)
		actualValue, err := converter.ToField("").DecodeDebeziumVariableDecimal(converted)
		assert.NoError(t, err)
		assert.Equal(t, "12.34", actualValue.String())
	}
}
