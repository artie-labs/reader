package converters

import (
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValueConverterForType(t *testing.T) {
	colName := "foo"
	{
		// Invalid
		_, err := ValueConverterForType(-1, nil)
		assert.ErrorContains(t, err, "unable get value converter for DataType(-1)")
	}
	{
		// bit
		{
			// bit(1)
			converter, err := ValueConverterForType(schema.Bit, &schema.Opts{
				Size: typing.ToPtr(1),
			})
			assert.NoError(t, err)
			assert.Equal(t, debezium.Field{Type: "boolean", FieldName: colName}, converter.ToField(colName))
		}
		{
			// bit(5)
			converter, err := ValueConverterForType(schema.Bit, &schema.Opts{
				Size: typing.ToPtr(5),
			})
			assert.NoError(t, err)
			assert.Equal(t, debezium.Field{Type: "bytes", FieldName: colName}, converter.ToField(colName))
		}
	}
	{
		// tinyint
		converter, err := ValueConverterForType(schema.TinyInt, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "int16", FieldName: colName}, converter.ToField(colName))
	}
	{
		// smallint
		converter, err := ValueConverterForType(schema.SmallInt, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "int16", FieldName: colName}, converter.ToField(colName))
	}
	{
		// mediumint
		converter, err := ValueConverterForType(schema.MediumInt, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "int32", FieldName: colName}, converter.ToField(colName))
	}
	{
		// int
		converter, err := ValueConverterForType(schema.Int, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "int32", FieldName: colName}, converter.ToField(colName))
	}
	{
		// bigint
		converter, err := ValueConverterForType(schema.BigInt, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "int64", FieldName: colName}, converter.ToField(colName))
	}
	{
		// float
		converter, err := ValueConverterForType(schema.Float, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "float", FieldName: colName}, converter.ToField(colName))
	}
	{
		// double
		converter, err := ValueConverterForType(schema.Double, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "double", FieldName: colName}, converter.ToField(colName))
	}
	{
		// decimal
		converter, err := ValueConverterForType(schema.Decimal, &schema.Opts{Scale: typing.ToPtr(uint16(3)), Precision: typing.ToPtr(5)})
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "bytes",
			DebeziumType: "org.apache.kafka.connect.data.Decimal",
			FieldName:    colName,
			Parameters: map[string]any{
				"scale":                     "3",
				"connect.decimal.precision": "5",
			},
		}, converter.ToField(colName))
	}
	{
		// Char
		converter, err := ValueConverterForType(schema.Char, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "string", FieldName: colName}, converter.ToField(colName))
	}
	{
		// Text
		converter, err := ValueConverterForType(schema.Text, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "string", FieldName: colName}, converter.ToField(colName))
	}
	{
		// Varchar
		converter, err := ValueConverterForType(schema.Varchar, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "string", FieldName: colName}, converter.ToField(colName))
	}
	{
		// Binary
		converter, err := ValueConverterForType(schema.Binary, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "bytes", FieldName: colName}, converter.ToField(colName))
	}
	{
		// Varbinary
		converter, err := ValueConverterForType(schema.Varbinary, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "bytes", FieldName: colName}, converter.ToField(colName))
	}
	{
		// Blob
		converter, err := ValueConverterForType(schema.Blob, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{Type: "bytes", FieldName: colName}, converter.ToField(colName))
	}
	{
		// Time
		converter, err := ValueConverterForType(schema.Time, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "int64",
			DebeziumType: "io.debezium.time.MicroTime",
			FieldName:    colName,
		}, converter.ToField(colName))
	}
	{
		// Date
		converter, err := ValueConverterForType(schema.Date, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "int32",
			DebeziumType: "io.debezium.time.Date",
			FieldName:    colName,
		}, converter.ToField(colName))
	}
	{
		// Datetime
		converter, err := ValueConverterForType(schema.DateTime, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "int64",
			DebeziumType: "io.debezium.time.MicroTimestamp",
			FieldName:    colName,
		}, converter.ToField(colName))
	}
	{
		// Timestamp
		converter, err := ValueConverterForType(schema.Timestamp, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "string",
			DebeziumType: "io.debezium.time.ZonedTimestamp",
			FieldName:    colName,
		}, converter.ToField(colName))
	}
	{
		// Year
		converter, err := ValueConverterForType(schema.Year, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "int32",
			DebeziumType: "io.debezium.time.Year",
			FieldName:    colName,
		}, converter.ToField(colName))
	}
	{
		// Enum
		converter, err := ValueConverterForType(schema.Enum, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "string",
			DebeziumType: "io.debezium.data.Enum",
			FieldName:    colName,
		}, converter.ToField(colName))
	}
	{
		// Set
		converter, err := ValueConverterForType(schema.Set, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "string",
			DebeziumType: "io.debezium.data.EnumSet",
			FieldName:    colName,
		}, converter.ToField(colName))
	}
	{
		// JSON
		converter, err := ValueConverterForType(schema.JSON, nil)
		assert.NoError(t, err)
		assert.Equal(t, debezium.Field{
			Type:         "string",
			DebeziumType: "io.debezium.data.Json",
			FieldName:    colName,
		}, converter.ToField(colName))
	}
}
