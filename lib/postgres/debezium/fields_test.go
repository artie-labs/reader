package debezium

import (
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestColumnToField(t *testing.T) {
	type _testCase struct {
		name     string
		colName  string
		dataType schema.DataType
		opts     *schema.Opts

		expected debezium.Field
	}

	testCases := []_testCase{
		{
			name:     "array",
			colName:  "foo",
			dataType: schema.Array,
			expected: debezium.Field{
				Type:      "array",
				FieldName: "foo",
			},
		},
		{
			name:     "text",
			colName:  "group",
			dataType: schema.Text,
			expected: debezium.Field{
				Type:      "string",
				FieldName: "group",
			},
		},
		{
			name:     "numeric",
			colName:  "numeric_col",
			dataType: schema.VariableNumeric,
			expected: debezium.Field{
				Type:         "struct",
				FieldName:    "numeric_col",
				DebeziumType: string(debezium.KafkaVariableNumericType),
			},
		},
		{
			name:     "numeric - with scale + precision",
			colName:  "numeric_col",
			dataType: schema.Numeric,
			opts: &schema.Opts{
				Scale:     ptr.ToString("2"),
				Precision: ptr.ToString("10"),
			},
			expected: debezium.Field{
				Type:         "",
				FieldName:    "numeric_col",
				DebeziumType: string(debezium.KafkaDecimalType),
				Parameters:   map[string]interface{}{"scale": "2", "connect.decimal.precision": "10"},
			},
		},
		{
			name:     "bit",
			colName:  "bit_col",
			dataType: schema.Bit,
			expected: debezium.Field{
				Type:      "boolean",
				FieldName: "bit_col",
			},
		},
		{
			name:     "bool",
			colName:  "bool_col",
			dataType: schema.Boolean,
			expected: debezium.Field{
				Type:      "boolean",
				FieldName: "bool_col",
			},
		},
		{
			name:     "interval",
			colName:  "interval_coL",
			dataType: schema.Interval,
			expected: debezium.Field{
				Type:         "int64",
				FieldName:    "interval_coL",
				DebeziumType: "io.debezium.time.MicroDuration",
			},
		},
		{
			name:     "time",
			colName:  "time",
			dataType: schema.Time,
			expected: debezium.Field{
				Type:         "int32",
				FieldName:    "time",
				DebeziumType: string(debezium.Time),
			},
		},
		{
			name:     "date",
			colName:  "date_col",
			dataType: schema.Date,
			expected: debezium.Field{
				Type:         "int32",
				FieldName:    "date_col",
				DebeziumType: string(debezium.Date),
			},
		},
		{
			name:     "char_text",
			colName:  "char_text_col",
			dataType: schema.TextThatRequiresEscaping,
			expected: debezium.Field{
				Type:      "string",
				FieldName: "char_text_col",
			},
		},
	}

	for _, testCase := range testCases {
		col := schema.Column{Name: testCase.colName, Type: testCase.dataType, Opts: testCase.opts}
		field := ColumnToField(col)
		assert.Equal(t, testCase.expected, field, testCase.name)
	}
}

func TestColumnsToFields(t *testing.T) {
	col1 := schema.Column{Name: "col1", Type: schema.Text}
	col2 := schema.Column{Name: "col2", Type: schema.Boolean}
	col3 := schema.Column{Name: "col3", Type: schema.Array}
	fields := ColumnsToFields([]schema.Column{col1, col2, col3})
	expected := []debezium.Field{
		{Type: "string", FieldName: "col1"},
		{Type: "boolean", FieldName: "col2"},
		{Type: "array", FieldName: "col3"},
	}
	assert.Equal(t, expected, fields)
}
