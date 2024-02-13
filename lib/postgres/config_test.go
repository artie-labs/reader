package postgres

import (
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
)

func TestPostgresConfig_Complete(t *testing.T) {
	type _testCase struct {
		name    string
		colName string
		colKind string

		expectedField        debezium.Field
		expectedEscColString string
	}

	testCases := []_testCase{
		{
			name:    "happy path",
			colName: "foo",
			colKind: "ARRAY",
			expectedField: debezium.Field{
				Type:      "array",
				FieldName: "foo",
			},
			expectedEscColString: `ARRAY_TO_JSON("foo")::TEXT as "foo"`,
		},
		{
			name:    "happy path (w/ reserved kw)",
			colName: "group",
			colKind: "character varying",
			expectedField: debezium.Field{
				Type:      "string",
				FieldName: "group",
			},
			expectedEscColString: `"group"`,
		},
		{
			name:    "numeric",
			colName: "numeric_col",
			colKind: "numeric",
			expectedField: debezium.Field{
				Type:         "struct",
				FieldName:    "numeric_col",
				DebeziumType: string(debezium.KafkaVariableNumericType),
			},
			expectedEscColString: `"numeric_col"`,
		},
		{
			name:    "bit",
			colName: "bit_col",
			colKind: "bit",
			expectedField: debezium.Field{
				Type:      "boolean",
				FieldName: "bit_col",
			},
			expectedEscColString: `"bit_col"`,
		},
		{
			name:    "bool",
			colName: "bool_col",
			colKind: "boolean",
			expectedField: debezium.Field{
				Type:      "boolean",
				FieldName: "bool_col",
			},
			expectedEscColString: `"bool_col"`,
		},
		{
			name:    "interval",
			colName: "interval_coL",
			colKind: "interval",
			expectedField: debezium.Field{
				Type:         "int64",
				FieldName:    "interval_coL",
				DebeziumType: "io.debezium.time.MicroDuration",
			},
			expectedEscColString: `cast(extract(epoch from "interval_coL")*1000000 as bigint) as "interval_coL"`,
		},
		{
			name:    "time with time zone",
			colName: "time_with_col",
			colKind: "time with time zone",
			expectedField: debezium.Field{
				Type:         "int32",
				FieldName:    "time_with_col",
				DebeziumType: string(debezium.Time),
			},
			expectedEscColString: `cast(extract(epoch from "time_with_col")*1000 as bigint) as "time_with_col"`,
		},
		{
			name:    "time without time zone",
			colName: "time_without_col",
			colKind: "time without time zone",
			expectedField: debezium.Field{
				Type:         "int32",
				FieldName:    "time_without_col",
				DebeziumType: string(debezium.Time),
			},
			expectedEscColString: `cast(extract(epoch from "time_without_col")*1000 as bigint) as "time_without_col"`,
		},
		{
			name:    "date",
			colName: "date_col",
			colKind: "date",
			expectedField: debezium.Field{
				Type:         "int32",
				FieldName:    "date_col",
				DebeziumType: string(debezium.Date),
			},
			expectedEscColString: `"date_col"`,
		},
		{
			name:    "char_text",
			colName: "char_text_col",
			colKind: "character",
			expectedField: debezium.Field{
				Type:      "string",
				FieldName: "char_text_col",
			},
			expectedEscColString: `"char_text_col"::text`,
		},
		{
			name:    "array (need to quote)",
			colName: "outcomeOrderTags",
			colKind: "ARRAY",
			expectedField: debezium.Field{
				Type:      "array",
				FieldName: "outcomeOrderTags",
			},
			expectedEscColString: `ARRAY_TO_JSON("outcomeOrderTags")::TEXT as "outcomeOrderTags"`,
		},
		{
			name:    "variable numeric",
			colName: "numeric_test",
			colKind: "numeric",
			expectedField: debezium.Field{
				Type:         "struct",
				FieldName:    "numeric_test",
				DebeziumType: string(debezium.KafkaVariableNumericType),
			},
			expectedEscColString: `"numeric_test"`,
		},
	}

	for _, testCase := range testCases {
		cfg := NewPostgresConfig()
		// TODO: Add test for hstore
		cfg.AddColumn(testCase.colName, testCase.colKind, nil, nil, nil)

		actualEscCol := cfg.GetColEscaped(testCase.colName)
		assert.Equal(t, testCase.expectedEscColString, actualEscCol, testCase.name)

		field, isOk := cfg.Fields.GetField(testCase.colName)
		assert.True(t, isOk, testCase.name)
		assert.Equal(t, testCase.expectedField, field, testCase.name)
	}
}
