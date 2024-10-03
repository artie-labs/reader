package schema

import (
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseColumnDataType(t *testing.T) {
	{
		// Array
		dataType, opts, err := parseColumnDataType("ARRAY", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Array, dataType)
		assert.Nil(t, opts)
	}
	{
		// String
		{
			// Character varying
			dataType, opts, err := parseColumnDataType("character varying", nil, nil, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, Text, dataType)
			assert.Nil(t, opts)
		}
		{
			// Character
			dataType, opts, err := parseColumnDataType("character", nil, nil, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, Text, dataType)
			assert.Nil(t, opts)
		}
	}
	{
		{
			// bit (char max length not specified)
			dataType, opts, err := parseColumnDataType("bit", nil, nil, nil, nil)
			assert.ErrorContains(t, err, "invalid bit column: missing character maximum length")
			assert.Equal(t, -1, int(dataType))
			assert.Nil(t, opts)
		}
		{
			// bit (1)
			dataType, opts, err := parseColumnDataType("bit", nil, nil, typing.ToPtr(1), nil)
			assert.NoError(t, err)
			assert.Equal(t, Bit, dataType)
			assert.Equal(t, 1, opts.CharMaxLength)
		}
		{
			// bit (5)
			dataType, opts, err := parseColumnDataType("bit", nil, nil, typing.ToPtr(5), nil)
			assert.NoError(t, err)
			assert.Equal(t, Bit, dataType)
			assert.Equal(t, 5, opts.CharMaxLength)
		}
	}
	{
		// boolean
		dataType, opts, err := parseColumnDataType("boolean", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Boolean, dataType)
		assert.Nil(t, opts)
	}
	{
		// interval
		dataType, opts, err := parseColumnDataType("interval", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Interval, dataType)
		assert.Nil(t, opts)
	}
	{
		// time with time zone
		dataType, opts, err := parseColumnDataType("time with time zone", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, TimeWithTimeZone, dataType)
		assert.Nil(t, opts)
	}
	{
		// time without time zone
		dataType, opts, err := parseColumnDataType("time without time zone", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Time, dataType)
		assert.Nil(t, opts)
	}
	{
		// date
		dataType, opts, err := parseColumnDataType("date", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Date, dataType)
		assert.Nil(t, opts)
	}
	{
		// inet
		dataType, opts, err := parseColumnDataType("inet", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Text, dataType)
		assert.Nil(t, opts)
	}
	{
		// numeric
		dataType, opts, err := parseColumnDataType("numeric", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, VariableNumeric, dataType)
		assert.Nil(t, opts)
	}
	{
		// numeric - with scale + precision
		dataType, opts, err := parseColumnDataType("numeric", typing.ToPtr(3), typing.ToPtr(uint16(2)), nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Numeric, dataType)
		assert.Equal(t, &Opts{Scale: 2, Precision: 3}, opts)
	}
	{
		// Variable numeric
		dataType, opts, err := parseColumnDataType("variable numeric", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, VariableNumeric, dataType)
		assert.Nil(t, opts)
	}
	{
		// Money
		dataType, opts, err := parseColumnDataType("money", nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, Money, dataType)
		assert.Nil(t, opts)
	}
	{
		// hstore
		dataType, opts, err := parseColumnDataType("user-defined", nil, nil, nil, typing.ToPtr("hstore"))
		assert.NoError(t, err)
		assert.Equal(t, HStore, dataType)
		assert.Nil(t, opts)
	}
	{
		// geometry
		dataType, opts, err := parseColumnDataType("user-defined", nil, nil, nil, typing.ToPtr("geometry"))
		assert.NoError(t, err)
		assert.Equal(t, Geometry, dataType)
		assert.Nil(t, opts)
	}
	{
		// geography
		dataType, opts, err := parseColumnDataType("user-defined", nil, nil, nil, typing.ToPtr("geography"))
		assert.NoError(t, err)
		assert.Equal(t, Geography, dataType)
		assert.Nil(t, opts)
	}
	{
		// user-defined text
		dataType, opts, err := parseColumnDataType("user-defined", nil, nil, nil, typing.ToPtr("foo"))
		assert.NoError(t, err)
		assert.Equal(t, UserDefinedText, dataType)
		assert.Nil(t, opts)
	}
	{
		// unsupported
		dataType, opts, err := parseColumnDataType("foo", nil, nil, nil, nil)
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
