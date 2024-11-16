package adapter

import (
	"github.com/artie-labs/transfer/lib/typing"
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

func TestMySQLAdapter_TableName(t *testing.T) {
	table := mysql.Table{
		Name: "table1",
	}
	adapter, err := buildMySQLAdapter(nil, "foo", table, []schema.Column{}, scan.ScannerConfig{})
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
			expected: "db.table1",
		},
		{
			table: mysql.Table{
				Name: "PublicStatus",
			},
			expected: "db.PublicStatus",
		},
	}

	for _, tc := range tcs {
		adapter, err := buildMySQLAdapter(nil, "db", tc.table, []schema.Column{}, scan.ScannerConfig{})
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, adapter.TopicSuffix())
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
			dataType:    -1,
			expectedErr: "unable get value converter for DataType(-1)",
		},
		{
			name:     "bit(1)",
			dataType: schema.Bit,
			opts: &schema.Opts{
				Size: typing.ToPtr(1),
			},
			expected: debezium.Field{
				Type:      "boolean",
				FieldName: colName,
			},
		},
		{
			name:     "bit(5)",
			dataType: schema.Bit,
			opts: &schema.Opts{
				Size: typing.ToPtr(5),
			},
			expected: debezium.Field{
				Type:      "bytes",
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
				Scale:     typing.ToPtr(uint16(3)),
				Precision: typing.ToPtr(5),
			},
			expected: debezium.Field{
				Type:         "bytes",
				DebeziumType: "org.apache.kafka.connect.data.Decimal",
				FieldName:    colName,
				Parameters: map[string]any{
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
				DebeziumType: "io.debezium.time.MicroTimestamp",
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
