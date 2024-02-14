package debezium

import (
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
)

func TestFields_AddField(t *testing.T) {
	type _testCase struct {
		name     string
		colName  string
		dataType DataType

		expectedField debezium.Field
	}

	testCases := []_testCase{
		{
			name:     "array",
			colName:  "foo",
			dataType: Array,
			expectedField: debezium.Field{
				Type:      "array",
				FieldName: "foo",
			},
		},
		{
			name:     "text",
			colName:  "group",
			dataType: Text,
			expectedField: debezium.Field{
				Type:      "string",
				FieldName: "group",
			},
		},
		{
			name:     "numeric",
			colName:  "numeric_col",
			dataType: VariableNumeric,
			expectedField: debezium.Field{
				Type:         "struct",
				FieldName:    "numeric_col",
				DebeziumType: string(debezium.KafkaVariableNumericType),
			},
		},
		{
			name:     "bit",
			colName:  "bit_col",
			dataType: Bit,
			expectedField: debezium.Field{
				Type:      "boolean",
				FieldName: "bit_col",
			},
		},
		{
			name:     "bool",
			colName:  "bool_col",
			dataType: Boolean,
			expectedField: debezium.Field{
				Type:      "boolean",
				FieldName: "bool_col",
			},
		},
		{
			name:     "interval",
			colName:  "interval_coL",
			dataType: Interval,
			expectedField: debezium.Field{
				Type:         "int64",
				FieldName:    "interval_coL",
				DebeziumType: "io.debezium.time.MicroDuration",
			},
		},
		{
			name:     "time",
			colName:  "time",
			dataType: Time,
			expectedField: debezium.Field{
				Type:         "int32",
				FieldName:    "time",
				DebeziumType: string(debezium.Time),
			},
		},
		{
			name:     "date",
			colName:  "date_col",
			dataType: Date,
			expectedField: debezium.Field{
				Type:         "int32",
				FieldName:    "date_col",
				DebeziumType: string(debezium.Date),
			},
		},
		{
			name:     "char_text",
			colName:  "char_text_col",
			dataType: TextThatRequiresEscaping,
			expectedField: debezium.Field{
				Type:      "string",
				FieldName: "char_text_col",
			},
		},
	}

	for _, testCase := range testCases {
		fields := NewFields()
		fields.AddField(testCase.colName, testCase.dataType, nil)

		field, isOk := fields.GetField(testCase.colName)
		assert.True(t, isOk, testCase.name)
		assert.Equal(t, testCase.expectedField, field, testCase.name)
	}
}
