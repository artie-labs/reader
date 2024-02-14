package postgres

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"

	pgDebezium "github.com/artie-labs/reader/lib/postgres/debezium"
)

func TestColKindToDataType(t *testing.T) {
	type _testCase struct {
		name      string
		colKind   string
		precision *string
		scale     *string
		udtName   *string

		expectedDataType pgDebezium.DataType
		expectedOpts     *pgDebezium.Opts
	}

	var testCases = []_testCase{
		{
			name:             "array",
			colKind:          "ARRAY",
			expectedDataType: pgDebezium.Array,
		},
		{
			name:             "character varying",
			colKind:          "character varying",
			expectedDataType: pgDebezium.Text,
		},
		{
			name:             "bit",
			colKind:          "bit",
			expectedDataType: pgDebezium.Bit,
		},
		{
			name:             "bool",
			colKind:          "boolean",
			expectedDataType: pgDebezium.Boolean,
		},
		{
			name:             "interval",
			colKind:          "interval",
			expectedDataType: pgDebezium.Interval,
		},
		{
			name:             "time with time zone",
			colKind:          "time with time zone",
			expectedDataType: pgDebezium.Time,
		},
		{
			name:             "time without time zone",
			colKind:          "time without time zone",
			expectedDataType: pgDebezium.Time,
		},
		{
			name:             "date",
			colKind:          "date",
			expectedDataType: pgDebezium.Date,
		},
		{
			name:             "char_text",
			colKind:          "character",
			expectedDataType: pgDebezium.TextThatRequiresEscaping,
		},
		{
			name:             "numeric",
			colKind:          "numeric",
			expectedDataType: pgDebezium.VariableNumeric,
		},
		{
			name:             "numeric - with scale + precision",
			colKind:          "numeric",
			scale:            ptr.ToString("2"),
			precision:        ptr.ToString("3"),
			expectedDataType: pgDebezium.Numeric,
			expectedOpts: &pgDebezium.Opts{
				Scale:     ptr.ToString("2"),
				Precision: ptr.ToString("3"),
			},
		},
		{
			name:             "variable numeric",
			colKind:          "variable numeric",
			expectedDataType: pgDebezium.VariableNumeric,
		},
		{
			name:             "money",
			colKind:          "money",
			expectedDataType: pgDebezium.Money,
			expectedOpts: &pgDebezium.Opts{
				Scale: ptr.ToString("2"), // money always has a scale of 2
			},
		},
		{
			name:             "hstore",
			colKind:          "user-defined",
			udtName:          ptr.ToString("hstore"),
			expectedDataType: pgDebezium.HStore,
		},
		{
			name:             "geometry",
			colKind:          "user-defined",
			udtName:          ptr.ToString("geometry"),
			expectedDataType: pgDebezium.Geometry,
		},
		{
			name:             "geography",
			colKind:          "user-defined",
			udtName:          ptr.ToString("geography"),
			expectedDataType: pgDebezium.Geography,
		},
		{
			name:             "user-defined text",
			colKind:          "user-defined",
			udtName:          ptr.ToString("foo"),
			expectedDataType: pgDebezium.UserDefinedText,
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
		dataType pgDebezium.DataType

		expected string
	}

	var testCases = []_testCase{
		{
			name:     "array",
			dataType: pgDebezium.Array,
			expected: `ARRAY_TO_JSON("foo")::TEXT as "foo"`,
		},
		{
			name:     "text",
			dataType: pgDebezium.Text,
			expected: `"foo"`,
		},
		{
			name:     "numeric",
			dataType: pgDebezium.Numeric,
			expected: `"foo"`,
		},
		{
			name:     "bit",
			dataType: pgDebezium.Bit,
			expected: `"foo"`,
		},
		{
			name:     "bool",
			dataType: pgDebezium.Boolean,
			expected: `"foo"`,
		},
		{
			name:     "interval",
			dataType: pgDebezium.Interval,
			expected: `cast(extract(epoch from "foo")*1000000 as bigint) as "foo"`,
		},
		{
			name:     "time",
			dataType: pgDebezium.Time,
			expected: `cast(extract(epoch from "foo")*1000 as bigint) as "foo"`,
		},
		{
			name:     "date",
			dataType: pgDebezium.Date,
			expected: `"foo"`,
		},
		{
			name:     "char_text",
			dataType: pgDebezium.TextThatRequiresEscaping,
			expected: `"foo"::text`,
		},
		{
			name:     "variable numeric",
			dataType: pgDebezium.VariableNumeric,
			expected: `"foo"`,
		},
	}

	for _, testCase := range testCases {
		actualEscCol := castColumn("foo", testCase.dataType)
		assert.Equal(t, testCase.expected, actualEscCol, testCase.name)
	}
}
