package debezium

import (
	"encoding/base64"
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
)

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
		actualScale := GetScale(testCase.value)
		assert.Equal(t, testCase.expectedScale, actualScale, testCase.name)
	}
}

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
