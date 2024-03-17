package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestCastColumn(t *testing.T) {
	type _testCase struct {
		name     string
		dataType schema.DataType

		expected    string
		expectedErr string
	}

	var testCases = []_testCase{
		{
			name:     "array",
			dataType: schema.Array,
			expected: `ARRAY_TO_JSON("foo")::TEXT as "foo"`,
		},
		{
			name:     "text",
			dataType: schema.Text,
			expected: `"foo"`,
		},
		{
			name:     "numeric",
			dataType: schema.Numeric,
			expected: `"foo"`,
		},
		{
			name:     "bit",
			dataType: schema.Bit,
			expected: `"foo"`,
		},
		{
			name:     "bool",
			dataType: schema.Boolean,
			expected: `"foo"`,
		},
		{
			name:     "time with time zone",
			dataType: schema.TimeWithTimeZone,
			expected: `"foo" AT TIME ZONE 'UTC' AS "foo"`,
		},
		{
			name:     "date",
			dataType: schema.Date,
			expected: `"foo"`,
		},
		{
			name:     "variable numeric",
			dataType: schema.VariableNumeric,
			expected: `"foo"`,
		},
	}

	for _, testCase := range testCases {
		actualEscCol, err := castColumn(schema.Column{Name: "foo", Type: testCase.dataType})
		if testCase.expectedErr == "" {
			assert.NoError(t, err, testCase.name)
			assert.Equal(t, testCase.expected, actualEscCol, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}
