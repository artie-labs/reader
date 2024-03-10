package adapter

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/transfer/lib/debezium"
)

func TestPostgresAdapter_TableName(t *testing.T) {
	table := postgres.Table{
		Schema: "schema",
		Name:   "table1",
	}
	assert.Equal(t, "table1", postgresAdapter{table: table}.TableName())
}

func TestPostgresAdapter_TopicSuffix(t *testing.T) {
	type _tc struct {
		table             postgres.Table
		expectedTopicName string
	}

	tcs := []_tc{
		{
			table: postgres.Table{
				Name:   "table1",
				Schema: "schema1",
			},
			expectedTopicName: "schema1.table1",
		},
		{
			table: postgres.Table{
				Name:   `"PublicStatus"`,
				Schema: "schema2",
			},
			expectedTopicName: "schema2.PublicStatus",
		},
	}

	for _, tc := range tcs {
		adapter := postgresAdapter{table: tc.table}
		assert.Equal(t, tc.expectedTopicName, adapter.TopicSuffix())
	}
}

func TestPostgresAdapter_PartitionKey(t *testing.T) {
	type _tc struct {
		name     string
		keys     []string
		row      map[string]any
		expected map[string]any
	}

	tcs := []_tc{
		{
			name:     "no primary keys",
			row:      map[string]any{},
			expected: map[string]any{},
		},
		{
			name:     "primary keys - empty row",
			keys:     []string{"foo", "bar"},
			row:      map[string]any{},
			expected: map[string]any{"foo": nil, "bar": nil},
		},
		{
			name:     "primary keys - row has data",
			keys:     []string{"foo", "bar"},
			row:      map[string]any{"foo": "a", "bar": 2, "baz": 3},
			expected: map[string]any{"foo": "a", "bar": 2},
		},
	}

	for _, tc := range tcs {
		table := postgres.Table{
			Schema:      "schema",
			Name:        "tbl1",
			PrimaryKeys: tc.keys,
		}
		adapter := postgresAdapter{table: table}
		assert.Equal(t, tc.expected, adapter.PartitionKey(tc.row), tc.name)
	}
}

func TestValueConverterForType_ToField(t *testing.T) {
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
				Scale:     2,
				Precision: 10,
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
		converter, err := valueConverterForType(testCase.dataType, testCase.opts)
		if testCase.expectedErr == "" {
			assert.NoError(t, err, testCase.name)
			field := converter.ToField(testCase.colName)
			assert.Equal(t, testCase.expected, field, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}

func TestValueConverterForType_Convert(t *testing.T) {
	type _tc struct {
		name          string
		col           schema.Column
		value         any
		numericValue  bool
		expectedValue any
		expectErr     bool
	}

	tcs := []_tc{
		{
			name:          "date (postgres.Date)",
			col:           schema.Column{Name: "date_col", Type: schema.Date},
			value:         time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC),
			expectedValue: int32(19480),
		},
		{
			name: "numeric (postgres.Numeric)",
			col: schema.Column{Name: "numeric_col", Type: schema.Numeric, Opts: &schema.Opts{
				Scale:     2,
				Precision: 5,
			}},
			value:         "578.01",
			numericValue:  true,
			expectedValue: "578.01",
		},
		{
			name:          "numeric (postgres.Numeric) - money",
			col:           schema.Column{Name: "money_col", Type: schema.Money},
			numericValue:  true,
			value:         123.99,
			expectedValue: "123.99",
		},
		{
			name:          "numeric (postgres.Numeric) - variable numeric",
			col:           schema.Column{Name: "variable_numeric_col", Type: schema.VariableNumeric},
			value:         "123.98",
			expectedValue: map[string]string{"scale": "2", "value": "MG4="},
		},
		{
			name:          "string",
			col:           schema.Column{Name: "name", Type: schema.Text},
			value:         "name",
			expectedValue: "name",
		},
		{
			name:          "boolean",
			col:           schema.Column{Name: "bool", Type: schema.Boolean},
			value:         true,
			expectedValue: true,
		},
		{
			name:          "json",
			col:           schema.Column{Name: "json", Type: schema.JSON},
			value:         `{"foo":"bar}`,
			expectedValue: `{"foo":"bar}`,
		},
	}

	for _, tc := range tcs {
		converter, err := valueConverterForType(tc.col.Type, tc.col.Opts)
		assert.NoError(t, err, tc.name)

		actualValue, actualErr := converter.Convert(tc.value)
		if tc.expectErr {
			assert.Error(t, actualErr, tc.name)
		} else {
			assert.NoError(t, actualErr, tc.name)
			if tc.numericValue {
				field := converter.ToField(tc.col.Name)
				val, err := field.DecodeDecimal(fmt.Sprint(actualValue))
				assert.NoError(t, err, tc.name)
				assert.Equal(t, tc.expectedValue, val.String(), tc.name)
			} else {
				assert.Equal(t, tc.expectedValue, actualValue, tc.name)
			}
		}
	}
}
