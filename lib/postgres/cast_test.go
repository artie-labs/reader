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

		expected string
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
			name:     "interval",
			dataType: schema.Interval,
			expected: `cast(extract(epoch from "foo")*1000000 as bigint) as "foo"`,
		},
		{
			name:     "time",
			dataType: schema.Time,
			expected: `cast(extract(epoch from "foo")*1000 as bigint) as "foo"`,
		},
		{
			name:     "date",
			dataType: schema.Date,
			expected: `"foo"`,
		},
		{
			name:     "char_text",
			dataType: schema.TextThatRequiresCasting,
			expected: `"foo"::text`,
		},
		{
			name:     "variable numeric",
			dataType: schema.VariableNumeric,
			expected: `"foo"`,
		},
	}

	for _, testCase := range testCases {
		actualEscCol := castColumn(schema.Column{Name: "foo", Type: testCase.dataType})
		assert.Equal(t, testCase.expected, actualEscCol, testCase.name)
	}
}
