package parse

import (
	"testing"
	"time"

	"github.com/artie-labs/reader/lib/mssql/schema"
	"github.com/stretchr/testify/assert"
)

func TestParseValue(t *testing.T) {
	{
		// Bit
		value, err := ParseValue(schema.Bit, true)
		assert.NoError(t, err)
		assert.Equal(t, true, value)

		_, err = ParseValue(schema.Bit, 1234)
		assert.ErrorContains(t, err, "expected bool got int with value: 1234")
	}
	{
		// Bytes
		value, err := ParseValue(schema.Bytes, []byte("test"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("test"), value)

		_, err = ParseValue(schema.Bytes, 1234)
		assert.ErrorContains(t, err, "expected []byte got int with value: 1234")
	}
	{
		for _, schemaDataType := range []schema.DataType{schema.Int16, schema.Int32, schema.Int64} {
			// Int16, Int32, Int64
			value, err := ParseValue(schemaDataType, int64(1234))
			assert.NoError(t, err, schemaDataType)
			assert.Equal(t, int64(1234), value, schemaDataType)

			_, err = ParseValue(schemaDataType, 1234)
			assert.ErrorContains(t, err, "expected int64 got int with value: 1234", schemaDataType)
		}
	}
	{
		// Numeric
		value, err := ParseValue(schema.Numeric, []uint8("1234"))
		assert.NoError(t, err)
		assert.Equal(t, "1234", value)
	}
	{
		// Floats
		value, err := ParseValue(schema.Float, float64(1234))
		assert.NoError(t, err)
		assert.Equal(t, float64(1234), value)
	}
	{
		// Money
		value, err := ParseValue(schema.Money, []uint8("1234"))
		assert.NoError(t, err)
		assert.Equal(t, "1234", value)
	}
	{
		// String
		value, err := ParseValue(schema.String, "test")
		assert.NoError(t, err)
		assert.Equal(t, "test", value)
	}
	{
		// UniqueIdentifier
		value, err := ParseValue(schema.UniqueIdentifier, []byte{172, 102, 115, 214, 125, 112, 173, 75, 151, 103, 38, 81, 105, 72, 207, 55})
		assert.NoError(t, err)
		assert.Equal(t, "D67366AC-707D-4BAD-9767-26516948CF37", value)
	}
	{
		// Date, Time, TimeMicro, TimeNano, Datetime2, Datetime2Micro, Datetime2Nano, DatetimeOffset
		schemaDataTypes := []schema.DataType{
			schema.Date,
			schema.Time, schema.TimeMicro, schema.TimeNano,
			schema.Datetime2, schema.Datetime2Micro, schema.Datetime2Nano,
			schema.DatetimeOffset,
		}

		for _, schemaDataType := range schemaDataTypes {
			value, err := ParseValue(schemaDataType, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
			assert.NoError(t, err, schemaDataType)
			assert.IsType(t, time.Time{}, value, schemaDataType)

			_, err = ParseValue(schemaDataType, 1234)
			assert.ErrorContains(t, err, "expected time.Time got int with value: 1234", schemaDataType)
		}
	}
}
