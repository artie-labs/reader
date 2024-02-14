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

		expected debezium.Field
	}

	testCases := []_testCase{
		{
			name:     "array",
			colName:  "foo",
			dataType: Array,
			expected: debezium.Field{
				Type:      "array",
				FieldName: "foo",
			},
		},
		{
			name:     "text",
			colName:  "group",
			dataType: Text,
			expected: debezium.Field{
				Type:      "string",
				FieldName: "group",
			},
		},
		{
			name:     "numeric",
			colName:  "numeric_col",
			dataType: VariableNumeric,
			expected: debezium.Field{
				Type:         "struct",
				FieldName:    "numeric_col",
				DebeziumType: string(debezium.KafkaVariableNumericType),
			},
		},
		{
			name:     "bit",
			colName:  "bit_col",
			dataType: Bit,
			expected: debezium.Field{
				Type:      "boolean",
				FieldName: "bit_col",
			},
		},
		{
			name:     "bool",
			colName:  "bool_col",
			dataType: Boolean,
			expected: debezium.Field{
				Type:      "boolean",
				FieldName: "bool_col",
			},
		},
		{
			name:     "interval",
			colName:  "interval_coL",
			dataType: Interval,
			expected: debezium.Field{
				Type:         "int64",
				FieldName:    "interval_coL",
				DebeziumType: "io.debezium.time.MicroDuration",
			},
		},
		{
			name:     "time",
			colName:  "time",
			dataType: Time,
			expected: debezium.Field{
				Type:         "int32",
				FieldName:    "time",
				DebeziumType: string(debezium.Time),
			},
		},
		{
			name:     "date",
			colName:  "date_col",
			dataType: Date,
			expected: debezium.Field{
				Type:         "int32",
				FieldName:    "date_col",
				DebeziumType: string(debezium.Date),
			},
		},
		{
			name:     "char_text",
			colName:  "char_text_col",
			dataType: TextThatRequiresEscaping,
			expected: debezium.Field{
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
		assert.Equal(t, testCase.expected, field, testCase.name)
	}
}
