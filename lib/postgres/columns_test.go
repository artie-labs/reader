package postgres

import (
	"testing"

	"github.com/artie-labs/reader/lib/postgres/debezium"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestColKindToDataType(t *testing.T) {
	type _testCase struct {
		name      string
		colKind   string
		precision *string
		scale     *string
		udtName   *string

		expectedDataType debezium.DataType
		expectedOpts     *debezium.Opts
	}

	var testCases = []_testCase{
		{
			name:             "array",
			colKind:          "ARRAY",
			expectedDataType: debezium.Array,
		},
		{
			name:             "character varying",
			colKind:          "character varying",
			expectedDataType: debezium.Text,
		},
		{
			name:             "bit",
			colKind:          "bit",
			expectedDataType: debezium.Bit,
		},
		{
			name:             "bool",
			colKind:          "boolean",
			expectedDataType: debezium.Boolean,
		},
		{
			name:             "interval",
			colKind:          "interval",
			expectedDataType: debezium.Interval,
		},
		{
			name:             "time with time zone",
			colKind:          "time with time zone",
			expectedDataType: debezium.Time,
		},
		{
			name:             "time without time zone",
			colKind:          "time without time zone",
			expectedDataType: debezium.Time,
		},
		{
			name:             "date",
			colKind:          "date",
			expectedDataType: debezium.Date,
		},
		{
			name:             "char_text",
			colKind:          "character",
			expectedDataType: debezium.TextThatRequiresEscaping,
		},
		{
			name:             "numeric",
			colKind:          "numeric",
			expectedDataType: debezium.VariableNumeric,
		},
		{
			name:             "numeric - with scale + precision",
			colKind:          "numeric",
			scale:            ptr.ToString("2"),
			precision:        ptr.ToString("3"),
			expectedDataType: debezium.Numeric,
			expectedOpts: &debezium.Opts{
				Scale:     ptr.ToString("2"),
				Precision: ptr.ToString("3"),
			},
		},
		{
			name:             "variable numeric",
			colKind:          "variable numeric",
			expectedDataType: debezium.VariableNumeric,
		},
		{
			name:             "money",
			colKind:          "money",
			expectedDataType: debezium.Money,
			expectedOpts: &debezium.Opts{
				Scale: ptr.ToString("2"), // money always has a scale of 2
			},
		},
		{
			name:             "hstore",
			colKind:          "user-defined",
			udtName:          ptr.ToString("hstore"),
			expectedDataType: debezium.HStore,
		},
		{
			name:             "geometry",
			colKind:          "user-defined",
			udtName:          ptr.ToString("geometry"),
			expectedDataType: debezium.Geometry,
		},
		{
			name:             "geography",
			colKind:          "user-defined",
			udtName:          ptr.ToString("geography"),
			expectedDataType: debezium.Geography,
		},
		{
			name:             "user-defined text",
			colKind:          "user-defined",
			udtName:          ptr.ToString("foo"),
			expectedDataType: debezium.UserDefinedText,
		},
	}

	for _, testCase := range testCases {
		dataType, opts := colKindToDataType(testCase.colKind, testCase.precision, testCase.scale, testCase.udtName)
		assert.Equal(t, testCase.expectedDataType, dataType, testCase.name)
		assert.Equal(t, testCase.expectedOpts, opts, testCase.name)
	}
}

func TestCastColumn(t *testing.T) {
	type _testCase struct {
		name     string
		dataType debezium.DataType

		expected string
	}

	var testCases = []_testCase{
		{
			name:     "array",
			dataType: debezium.Array,
			expected: `ARRAY_TO_JSON("foo")::TEXT as "foo"`,
		},
		{
			name:     "text",
			dataType: debezium.Text,
			expected: `"foo"`,
		},
		{
			name:     "numeric",
			dataType: debezium.Numeric,
			expected: `"foo"`,
		},
		{
			name:     "bit",
			dataType: debezium.Bit,
			expected: `"foo"`,
		},
		{
			name:     "bool",
			dataType: debezium.Boolean,
			expected: `"foo"`,
		},
		{
			name:     "interval",
			dataType: debezium.Interval,
			expected: `cast(extract(epoch from "foo")*1000000 as bigint) as "foo"`,
		},
		{
			name:     "time",
			dataType: debezium.Time,
			expected: `cast(extract(epoch from "foo")*1000 as bigint) as "foo"`,
		},
		{
			name:     "date",
			dataType: debezium.Date,
			expected: `"foo"`,
		},
		{
			name:     "char_text",
			dataType: debezium.TextThatRequiresEscaping,
			expected: `"foo"::text`,
		},
		{
			name:     "variable numeric",
			dataType: debezium.VariableNumeric,
			expected: `"foo"`,
		},
	}

	for _, testCase := range testCases {
		actualEscCol := castColumn("foo", testCase.dataType)
		assert.Equal(t, testCase.expected, actualEscCol, testCase.name)
	}
}
