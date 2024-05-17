package schema

import (
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseColumnDataType(t *testing.T) {
	{
		// bit
		dataType, opts, err := ParseColumnDataType("bit", nil, nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, opts)
		assert.Equal(t, Bit, dataType)
	}
	{
		// smallint and tinyint
		for _, colKind := range []string{"smallint", "tinyint"} {
			dataType, opts, err := ParseColumnDataType(colKind, nil, nil, nil)
			assert.NoError(t, err, colKind)
			assert.Nil(t, opts, colKind)
			assert.Equal(t, Int16, dataType, colKind)
		}
	}
	{
		// int
		dataType, opts, err := ParseColumnDataType("int", nil, nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, opts)
		assert.Equal(t, Int32, dataType)
	}
	{
		// bigint
		dataType, opts, err := ParseColumnDataType("bigint", nil, nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, opts)
		assert.Equal(t, Int64, dataType)
	}
	{
		// float and real
		for _, colKind := range []string{"float", "real"} {
			dataType, opts, err := ParseColumnDataType(colKind, nil, nil, nil)
			assert.NoError(t, err, colKind)
			assert.Nil(t, opts, colKind)
			assert.Equal(t, Float, dataType, colKind)
		}
	}
	{
		// smallmoney and money
		for _, colKind := range []string{"smallmoney", "money"} {
			dataType, opts, err := ParseColumnDataType(colKind, nil, nil, nil)
			assert.NoError(t, err, colKind)
			assert.Nil(t, opts, colKind)
			assert.Equal(t, Money, dataType, colKind)
		}
	}
	{
		// numeric and decimal
		{
			// valid
			for _, colKind := range []string{"numeric", "decimal"} {
				dataType, opts, err := ParseColumnDataType(colKind, ptr.ToInt(1), ptr.ToInt(2), nil)
				assert.NoError(t, err, colKind)
				assert.NotNil(t, opts, colKind)
				assert.Equal(t, Numeric, dataType, colKind)
				assert.Equal(t, 2, opts.Scale, colKind)
				assert.Equal(t, 1, opts.Precision, colKind)
			}
		}
		{
			// invalid, precision is missing
			for _, colKind := range []string{"numeric", "decimal"} {
				dataType, opts, err := ParseColumnDataType(colKind, nil, ptr.ToInt(2), nil)
				assert.ErrorContains(t, err, colKind)
				assert.Nil(t, opts, colKind)
				assert.Equal(t, -1, dataType, colKind)
			}
		}
	}
}

func TestBuildPkValuesQuery(t *testing.T) {
	{
		query := buildPkValuesQuery(
			[]Column{
				{Name: "a", Type: String},
				{Name: "b", Type: String},
				{Name: "c", Type: String},
			},
			"schema",
			"table",
			false,
		)
		assert.Equal(t, `SELECT TOP 1 "a","b","c" FROM "schema"."table" ORDER BY "a","b","c"`, query)
	}
	{
		// Descending
		query := buildPkValuesQuery(
			[]Column{
				{Name: "a", Type: String},
				{Name: "b", Type: String},
				{Name: "c", Type: String},
			},
			"schema",
			"table",
			true,
		)
		assert.Equal(t, `SELECT TOP 1 "a","b","c" FROM "schema"."table" ORDER BY "a" DESC,"b" DESC,"c" DESC`, query)
	}
}
