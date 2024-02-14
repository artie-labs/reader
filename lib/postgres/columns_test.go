package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pgDebezium "github.com/artie-labs/reader/lib/postgres/debezium"
)

func TestColKindToDataType(t *testing.T) {
	type _testCase struct {
		name    string
		colKind string

		expectedDataType pgDebezium.DataType
	}

	var testCases = []_testCase{
		{
			name:             "happy path",
			colKind:          "ARRAY",
			expectedDataType: pgDebezium.Array,
		},
		{
			name:             "happy path",
			colKind:          "character varying",
			expectedDataType: pgDebezium.Text,
		},
		{
			name:             "numeric",
			colKind:          "numeric",
			expectedDataType: pgDebezium.VariableNumeric,
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
			name:             "variable numeric",
			colKind:          "numeric",
			expectedDataType: pgDebezium.VariableNumeric,
		},
	}

	for _, testCase := range testCases {
		// TODO: Add test for hstore
		dataType, opts := colKindToDataType(testCase.colKind, nil, nil, nil)
		assert.Equal(t, testCase.expectedDataType, dataType, testCase.name)
		assert.Nil(t, opts, testCase.name)
	}
}

func TestCastColumn(t *testing.T) {
	type _testCase struct {
		name     string
		dataType pgDebezium.DataType

		expectedCastColumn string
	}

	var testCases = []_testCase{
		{
			name:               "array",
			dataType:           pgDebezium.Array,
			expectedCastColumn: `ARRAY_TO_JSON("foo")::TEXT as "foo"`,
		},
		{
			name:               "text",
			dataType:           pgDebezium.Text,
			expectedCastColumn: `"foo"`,
		},
		{
			name:               "numeric",
			dataType:           pgDebezium.Numeric,
			expectedCastColumn: `"foo"`,
		},
		{
			name:               "bit",
			dataType:           pgDebezium.Bit,
			expectedCastColumn: `"foo"`,
		},
		{
			name:               "bool",
			dataType:           pgDebezium.Boolean,
			expectedCastColumn: `"foo"`,
		},
		{
			name:               "interval",
			dataType:           pgDebezium.Interval,
			expectedCastColumn: `cast(extract(epoch from "foo")*1000000 as bigint) as "foo"`,
		},
		{
			name:               "time",
			dataType:           pgDebezium.Time,
			expectedCastColumn: `cast(extract(epoch from "foo")*1000 as bigint) as "foo"`,
		},
		{
			name:               "date",
			dataType:           pgDebezium.Date,
			expectedCastColumn: `"foo"`,
		},
		{
			name:               "char_text",
			dataType:           pgDebezium.TextThatRequiresEscaping,
			expectedCastColumn: `"foo"::text`,
		},
		{
			name:               "variable numeric",
			dataType:           pgDebezium.VariableNumeric,
			expectedCastColumn: `"foo"`,
		},
	}

	for _, testCase := range testCases {
		actualEscCol := castColumn("foo", testCase.dataType)
		assert.Equal(t, testCase.expectedCastColumn, actualEscCol, testCase.name)
	}
}
