package converters

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecimalToBase64(t *testing.T) {
	type _tc struct {
		name  string
		value string
		scale int
	}

	tcs := []_tc{
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
	}

	for _, tc := range tcs {
		actualEncodedValue := EncodeDecimalToBytes(tc.value, tc.scale)
		field := debezium.Field{
			Parameters: map[string]any{
				"scale": tc.scale,
			},
		}

		actualValue, err := field.DecodeDecimal(base64.StdEncoding.EncodeToString(actualEncodedValue))
		assert.NoError(t, err, tc.name)
		assert.Equal(t, tc.value, actualValue.String(), tc.name)
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
		convertedMap, ok := converted.(VariableScaleDecimal)
		assert.True(t, ok)
		assert.Equal(t, VariableScaleDecimal{Scale: 2, Value: []byte{0x4, 0xd2}}, convertedMap)

		decimalConverter := NewDecimalConverter(int(convertedMap.Scale), nil).ToField("")
		actualValue, err := decimalConverter.DecodeDecimal(base64.StdEncoding.EncodeToString(convertedMap.Value))
		assert.NoError(t, err)
		assert.Equal(t, "12.34", fmt.Sprint(actualValue))
	}
}
