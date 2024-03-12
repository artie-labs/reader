package scanner

import (
	"testing"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/stretchr/testify/assert"
)

func TestScanAdapter_ParsePrimaryKeyValue(t *testing.T) {
	{
		// Column does not exist
		adapter := scanAdapter{columns: []schema.Column{{Name: "bar"}}}
		_, err := adapter.ParsePrimaryKeyValue("foo", "1234")
		assert.ErrorContains(t, err, "primary key column does not exist: foo")
	}

	testCases := []struct {
		name        string
		dataType    schema.DataType
		value       string
		expected    any
		expectedErr string
	}{
		{
			name:        "unsupported data type",
			dataType:    schema.Timestamp,
			value:       "1234",
			expectedErr: "DataType(20) for column 'col' is not supported for use as a primary key",
		},
		{
			name:        "boolean - malformed",
			dataType:    schema.Boolean,
			value:       "1234",
			expectedErr: `unable to convert "1234" to a bool`,
		},
		{
			name:     "boolean - valid",
			dataType: schema.Boolean,
			value:    "true",
			expected: true,
		},
		{
			name:        "int16 - malformed",
			dataType:    schema.Int16,
			value:       "true",
			expectedErr: `unable to convert "true" to an int16`,
		},
		{
			name:     "int16 - valid",
			dataType: schema.Int16,
			value:    "3412",
			expected: int16(3412),
		},
		{
			name:        "int32 - malformed",
			dataType:    schema.Int32,
			value:       "apple",
			expectedErr: `unable to convert "apple" to an int32`,
		},
		{
			name:     "int32 - valid",
			dataType: schema.Int32,
			value:    "32768",
			expected: int32(32768),
		},
		{
			name:        "int64 - malformed",
			dataType:    schema.Int64,
			value:       "orange",
			expectedErr: `unable to convert "orange" to an int64`,
		},
		{
			name:     "int64 - valid",
			dataType: schema.Int64,
			value:    "2147483647",
			expected: int64(2_147_483_647),
		},
		{
			name:        "real - malformed",
			dataType:    schema.Real,
			value:       "orange",
			expectedErr: `unable to convert "orange" to a float32`,
		},
		{
			name:     "real - valid",
			dataType: schema.Real,
			value:    "123445.79",
			expected: float32(12_3445.79),
		},
		{
			name:        "double - malformed",
			dataType:    schema.Double,
			value:       "orange",
			expectedErr: `unable to convert "orange" to a float64`,
		},
		{
			name:     "double - valid",
			dataType: schema.Double,
			value:    "3.141592653589793",
			expected: float64(3.141592653589793),
		},
		{
			name:     "text - valid",
			dataType: schema.Text,
			value:    "one two three four",
			expected: "one two three four",
		},
	}

	for _, testCase := range testCases {
		adapter := scanAdapter{columns: []schema.Column{{Name: "col", Type: testCase.dataType}}}
		value, err := adapter.ParsePrimaryKeyValue("col", testCase.value)
		if testCase.expectedErr == "" {
			assert.NoError(t, err, testCase.name)
			assert.Equal(t, testCase.expected, value, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}
