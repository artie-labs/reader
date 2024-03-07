package adapter

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

		expected    debezium.Field
		expectedErr string
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
				Parameters:   map[string]any{"scale": "2", "connect.decimal.precision": "10"},
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
			name:     "inet",
			colName:  "inet_col",
			dataType: schema.Inet,
			expected: debezium.Field{
				Type:      "string",
				FieldName: "inet_col",
			},
		},
		{
			name:        "unsupported data type",
			colName:     "inet_col",
			dataType:    -1,
			expectedErr: "unsupported data type: DataType(-1)",
		},
	}

	for _, testCase := range testCases {
		col := schema.Column{Name: testCase.colName, Type: testCase.dataType, Opts: testCase.opts}
		field, err := ColumnToField(col)
		if testCase.expectedErr == "" {
			assert.NoError(t, err, testCase.name)
			assert.Equal(t, testCase.expected, field, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}
