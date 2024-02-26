package adapter

import (
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

func TestMySQLAdapter_TableName(t *testing.T) {
	table := mysql.Table{
		Name: "table1",
	}
	adapter, err := NewMySQLAdapter(table)
	assert.NoError(t, err)
	assert.Equal(t, "table1", adapter.TableName())
}

func TestMySQLAdapter_TopicSuffix(t *testing.T) {
	type _tc struct {
		table    mysql.Table
		expected string
	}

	tcs := []_tc{
		{
			table: mysql.Table{
				Name: "table1",
			},
			expected: "table1",
		},
		{
			table: mysql.Table{
				Name: `"PublicStatus"`,
			},
			expected: "PublicStatus",
		},
	}

	for _, tc := range tcs {
		adapter, err := NewMySQLAdapter(tc.table)
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, adapter.TopicSuffix())
	}
}

func TestMySQLAdapter_Fields(t *testing.T) {
	table := mysql.Table{
		Name: "table1",
		Columns: []schema.Column{
			{Name: "col1", Type: schema.Text},
			{Name: "col2", Type: schema.BigInt},
			{Name: "col3", Type: schema.JSON},
		},
	}
	adapter, err := NewMySQLAdapter(table)
	assert.NoError(t, err)
	expected := []debezium.Field{
		{FieldName: "col1", Type: "string"},
		{FieldName: "col2", Type: "int64"},
		{FieldName: "col3", Type: "string", DebeziumType: "io.debezium.data.Json"},
	}
	assert.Equal(t, expected, adapter.Fields())
}

func TestMySQLAdapter_PartitionKey(t *testing.T) {
	type _tc struct {
		name     string
		keys     []primary_key.Key
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
			keys:     []primary_key.Key{{Name: "foo"}, {Name: "bar"}},
			row:      map[string]any{},
			expected: map[string]any{"foo": nil, "bar": nil},
		},
		{
			name:     "primary keys - row has data",
			keys:     []primary_key.Key{{Name: "foo"}, {Name: "bar"}},
			row:      map[string]any{"foo": "a", "bar": 2, "baz": 3},
			expected: map[string]any{"foo": "a", "bar": 2},
		},
	}

	for _, tc := range tcs {
		table := mysql.NewTable("tbl1")
		table.PrimaryKeys = tc.keys
		adapter, err := NewMySQLAdapter(*table)
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, adapter.PartitionKey(tc.row), tc.name)
	}
}

func TestValueConverterForType(t *testing.T) {
	colName := "the_col"

	type _tc struct {
		name     string
		dataType schema.DataType
		opts     *schema.Opts

		expected    debezium.Field
		expectedErr string
	}

	tcs := []_tc{
		{
			name:        "invalid data type",
			dataType:    schema.InvalidDataType,
			expectedErr: "unable get value converter for DataType[0]",
		},
		{
			name:     "bit",
			dataType: schema.Bit,
			expected: debezium.Field{
				Type:      "boolean",
				FieldName: colName,
			},
		},
		{
			name:     "tinyint",
			dataType: schema.TinyInt,
			expected: debezium.Field{
				Type:      "int16",
				FieldName: colName,
			},
		},
		{
			name:     "smallint",
			dataType: schema.SmallInt,
			expected: debezium.Field{
				Type:      "int16",
				FieldName: colName,
			},
		},
		{
			name:     "mediumint",
			dataType: schema.MediumInt,
			expected: debezium.Field{
				Type:      "int32",
				FieldName: colName,
			},
		},
		{
			name:     "int",
			dataType: schema.Int,
			expected: debezium.Field{
				Type:      "int32",
				FieldName: colName,
			},
		},
		{
			name:     "bigint",
			dataType: schema.BigInt,
			expected: debezium.Field{
				Type:      "int64",
				FieldName: colName,
			},
		},
		{
			name:     "float",
			dataType: schema.Float,
			expected: debezium.Field{
				Type:      "float",
				FieldName: colName,
			},
		},
		{
			name:     "double",
			dataType: schema.Double,
			expected: debezium.Field{
				Type:      "double",
				FieldName: colName,
			},
		},
		{
			name:     "decimal",
			dataType: schema.Decimal,
			opts: &schema.Opts{
				Scale:     ptr.ToInt(3),
				Precision: ptr.ToInt(5),
			},
			expected: debezium.Field{
				DebeziumType: "org.apache.kafka.connect.data.Decimal",
				FieldName:    colName,
				Parameters: map[string]interface{}{
					"scale":                     "3",
					"connect.decimal.precision": "5",
				},
			},
		},
		{
			name:     "char",
			dataType: schema.Char,
			expected: debezium.Field{
				Type:      "string",
				FieldName: colName,
			},
		},
		{
			name:     "text",
			dataType: schema.Text,
			expected: debezium.Field{
				Type:      "string",
				FieldName: colName,
			},
		},
		{
			name:     "varchar",
			dataType: schema.Varchar,
			expected: debezium.Field{
				Type:      "string",
				FieldName: colName,
			},
		},
		{
			name:     "binary",
			dataType: schema.Binary,
			expected: debezium.Field{
				Type:      "bytes",
				FieldName: colName,
			},
		},
		{
			name:     "varbinary",
			dataType: schema.Varbinary,
			expected: debezium.Field{
				Type:      "bytes",
				FieldName: colName,
			},
		},
		{
			name:     "blob",
			dataType: schema.Blob,
			expected: debezium.Field{
				Type:      "bytes",
				FieldName: colName,
			},
		},
		{
			name:     "time",
			dataType: schema.Time,
			expected: debezium.Field{
				Type:         "int64",
				DebeziumType: "io.debezium.time.MicroTime",
				FieldName:    colName,
			},
		},
		{
			name:     "date",
			dataType: schema.Date,
			expected: debezium.Field{
				Type:         "int32",
				DebeziumType: "io.debezium.time.Date",
				FieldName:    colName,
			},
		},
		{
			name:     "datetime",
			dataType: schema.DateTime,
			expected: debezium.Field{
				Type:         "int64",
				DebeziumType: "io.debezium.time.Timestamp",
				FieldName:    colName,
			},
		},
		{
			name:     "timestamp",
			dataType: schema.Timestamp,
			expected: debezium.Field{
				Type:         "string",
				DebeziumType: "io.debezium.time.ZonedTimestamp",
				FieldName:    colName,
			},
		},
		{
			name:     "year",
			dataType: schema.Year,
			expected: debezium.Field{
				Type:         "int32",
				DebeziumType: "io.debezium.time.Year",
				FieldName:    colName,
			},
		},
		{
			name:     "enum",
			dataType: schema.Enum,
			expected: debezium.Field{
				Type:         "string",
				DebeziumType: "io.debezium.data.Enum",
				FieldName:    colName,
			},
		},
		{
			name:     "set",
			dataType: schema.Set,
			expected: debezium.Field{
				Type:         "string",
				DebeziumType: "io.debezium.data.EnumSet",
				FieldName:    colName,
			},
		},
		{
			name:     "json",
			dataType: schema.JSON,
			expected: debezium.Field{
				Type:         "string",
				DebeziumType: "io.debezium.data.Json",
				FieldName:    colName,
			},
		},
	}

	for _, tc := range tcs {
		converter, err := valueConverterForType(tc.dataType, tc.opts)
		if tc.expectedErr == "" {
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, converter.ToField(colName), tc.name)
		} else {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		}

	}
}
