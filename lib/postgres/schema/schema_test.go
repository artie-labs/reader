package schema

import (
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseColumnDataType(t *testing.T) {
	{
		// Array
		dataType, opts, err := ParseColumnDataType("ARRAY", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Array, dataType)
		assert.Nil(t, opts)
	}
	{
		// String
		{
			// Character varying
			dataType, opts, err := ParseColumnDataType("character varying", nil, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, Text, dataType)
			assert.Nil(t, opts)
		}
		{
			// Character
			dataType, opts, err := ParseColumnDataType("character", nil, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, Text, dataType)
			assert.Nil(t, opts)
		}
	}
	{
		// bit
		dataType, opts, err := ParseColumnDataType("bit", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Bit, dataType)
		assert.Nil(t, opts)
	}
	{
		// boolean
		dataType, opts, err := ParseColumnDataType("boolean", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Boolean, dataType)
		assert.Nil(t, opts)
	}
	{
		// interval
		dataType, opts, err := ParseColumnDataType("interval", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Interval, dataType)
		assert.Nil(t, opts)
	}
	{
		// time with time zone
		dataType, opts, err := ParseColumnDataType("time with time zone", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, TimeWithTimeZone, dataType)
		assert.Nil(t, opts)
	}
	{
		// time without time zone
		dataType, opts, err := ParseColumnDataType("time without time zone", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Time, dataType)
		assert.Nil(t, opts)
	}
	{
		// date
		dataType, opts, err := ParseColumnDataType("date", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Date, dataType)
		assert.Nil(t, opts)
	}
	{
		// inet
		dataType, opts, err := ParseColumnDataType("inet", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Text, dataType)
		assert.Nil(t, opts)
	}
	{
		// numeric
		dataType, opts, err := ParseColumnDataType("numeric", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, VariableNumeric, dataType)
		assert.Nil(t, opts)
	}
	{
		// numeric - with scale + precision
		dataType, opts, err := ParseColumnDataType("numeric", typing.ToPtr(3), typing.ToPtr(uint16(2)), nil)
		assert.NoError(t, err)
		assert.Equal(t, Numeric, dataType)
		assert.Equal(t, &Opts{Scale: 2, Precision: 3}, opts)
	}
	{
		// Variable numeric
		dataType, opts, err := ParseColumnDataType("variable numeric", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, VariableNumeric, dataType)
		assert.Nil(t, opts)
	}
	{
		// Money
		dataType, opts, err := ParseColumnDataType("money", nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Money, dataType)
		assert.Nil(t, opts)
	}
	{
		// hstore
		dataType, opts, err := ParseColumnDataType("user-defined", nil, nil, typing.ToPtr("hstore"))
		assert.NoError(t, err)
		assert.Equal(t, HStore, dataType)
		assert.Nil(t, opts)
	}
	{
		// geometry
		dataType, opts, err := ParseColumnDataType("user-defined", nil, nil, typing.ToPtr("geometry"))
		assert.NoError(t, err)
		assert.Equal(t, Geometry, dataType)
		assert.Nil(t, opts)
	}
	{
		// geography
		dataType, opts, err := ParseColumnDataType("user-defined", nil, nil, typing.ToPtr("geography"))
		assert.NoError(t, err)
		assert.Equal(t, Geography, dataType)
		assert.Nil(t, opts)
	}
	{
		// user-defined text
		dataType, opts, err := ParseColumnDataType("user-defined", nil, nil, typing.ToPtr("foo"))
		assert.NoError(t, err)
		assert.Equal(t, UserDefinedText, dataType)
		assert.Nil(t, opts)
	}
	{
		// unsupported
		dataType, opts, err := ParseColumnDataType("foo", nil, nil, nil)
		assert.ErrorContains(t, err, `unknown data type: "foo"`)
		assert.Equal(t, -1, int(dataType))
		assert.Nil(t, opts)
	}
}

func TestBuildPkValuesQuery(t *testing.T) {
	{
		query := buildPkValuesQuery(buildPkValuesQueryArgs{
			Keys: []Column{
				{Name: "a", Type: Text},
				{Name: "b", Type: Text},
				{Name: "c", Type: Text},
			},
			Schema:    "schema",
			TableName: "table",
		})
		assert.Equal(t, `SELECT "a","b","c" FROM "schema"."table" ORDER BY "a","b","c" LIMIT 1`, query)
	}
	// Descending
	{
		query := buildPkValuesQuery(buildPkValuesQueryArgs{
			Keys: []Column{
				{Name: "a", Type: Text},
				{Name: "b", Type: Text},
				{Name: "c", Type: Text},
			},
			Schema:     "schema",
			TableName:  "table",
			Descending: true,
		})
		assert.Equal(t, `SELECT "a","b","c" FROM "schema"."table" ORDER BY "a" DESC,"b" DESC,"c" DESC LIMIT 1`, query)
	}
}
