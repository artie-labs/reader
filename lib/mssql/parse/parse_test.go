package parse

import (
	"testing"

	"github.com/artie-labs/reader/lib/mssql/schema"
	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
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
}
