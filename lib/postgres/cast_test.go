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
			name:     "time with time zone",
			dataType: schema.TimeWithTimeZone,
			expected: `"foo" AT TIME ZONE 'UTC' AS "foo"`,
		},
	}

	for _, testCase := range testCases {
		actualEscCol, err := castColumn(schema.Column{Name: "foo", Type: testCase.dataType})
		assert.NoError(t, err, testCase.name)
		assert.Equal(t, testCase.expected, actualEscCol, testCase.name)
	}
}
