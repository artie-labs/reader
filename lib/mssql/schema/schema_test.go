package schema

import (
	"github.com/artie-labs/transfer/lib/typing"
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
				dataType, opts, err := ParseColumnDataType(colKind, typing.ToPtr(1), typing.ToPtr(uint16(2)), nil)
				assert.NoError(t, err, colKind)
				assert.NotNil(t, opts, colKind)
				assert.Equal(t, Numeric, dataType, colKind)
				assert.Equal(t, uint16(2), opts.Scale, colKind)
				assert.Equal(t, 1, opts.Precision, colKind)
			}
		}
		{
			// invalid, precision is missing
			for _, colKind := range []string{"numeric", "decimal"} {
				dataType, opts, err := ParseColumnDataType(colKind, nil, typing.ToPtr(uint16(2)), nil)
				assert.ErrorContains(t, err, "expected precision and scale to be not-nil", colKind)
				assert.Nil(t, opts, colKind)
				assert.Equal(t, -1, int(dataType), colKind)
			}
		}
	}
	{
		// time
		{
			// Default
			for i := 0; i <= 3; i++ {
				dataType, opts, err := ParseColumnDataType("time", nil, nil, typing.ToPtr(i))
				assert.NoError(t, err, i)
				assert.Nil(t, opts, i)
				assert.Equal(t, Time, dataType, i)
			}
		}
		{
			// Micro
			for i := 4; i <= 6; i++ {
				dataType, opts, err := ParseColumnDataType("time", nil, nil, typing.ToPtr(i))
				assert.NoError(t, err, i)
				assert.Nil(t, opts, i)
				assert.Equal(t, TimeMicro, dataType, i)
			}
		}
		{
			// Nano
			dataType, opts, err := ParseColumnDataType("time", nil, nil, typing.ToPtr(7))
			assert.NoError(t, err)
			assert.Nil(t, opts)
			assert.Equal(t, TimeNano, dataType)
		}
		{
			// Invalid
			for _, invalidNumbers := range []int{-1, 8, 9} {
				dataType, opts, err := ParseColumnDataType("time", nil, nil, &invalidNumbers)
				assert.ErrorContains(t, err, "invalid datetime precision")
				assert.Nil(t, opts)
				assert.Equal(t, -1, int(dataType))
			}
		}
	}
	{
		// date
		dataType, opts, err := ParseColumnDataType("date", nil, nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, opts)
		assert.Equal(t, Date, dataType)
	}
	{
		// Datetime
		for _, colKind := range []string{"smalldatetime", "datetime"} {
			dataType, opts, err := ParseColumnDataType(colKind, nil, nil, nil)
			assert.NoError(t, err, colKind)
			assert.Nil(t, opts, colKind)
			assert.Equal(t, Datetime2, dataType, colKind)
		}
	}
	{
		// Datetime2
		{
			// Default
			for i := 0; i <= 3; i++ {
				dataType, opts, err := ParseColumnDataType("datetime2", nil, nil, typing.ToPtr(i))
				assert.NoError(t, err, i)
				assert.Nil(t, opts, i)
				assert.Equal(t, Datetime2, dataType, i)
			}
		}
		{
			// Micro
			for i := 4; i <= 6; i++ {
				dataType, opts, err := ParseColumnDataType("datetime2", nil, nil, typing.ToPtr(i))
				assert.NoError(t, err, i)
				assert.Nil(t, opts, i)
				assert.Equal(t, Datetime2Micro, dataType, i)
			}
		}
		{
			// nano
			dataType, opts, err := ParseColumnDataType("datetime2", nil, nil, typing.ToPtr(7))
			assert.NoError(t, err)
			assert.Nil(t, opts)
			assert.Equal(t, Datetime2Nano, dataType)
		}
		{
			// Invalid
			for _, invalidNumbers := range []int{-1, 8, 9} {
				dataType, opts, err := ParseColumnDataType("datetime2", nil, nil, &invalidNumbers)
				assert.ErrorContains(t, err, "invalid datetime precision")
				assert.Nil(t, opts)
				assert.Equal(t, -1, int(dataType))
			}
		}
	}
	{
		// datetimeoffset
		dataType, opts, err := ParseColumnDataType("datetimeoffset", nil, nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, opts)
		assert.Equal(t, DatetimeOffset, dataType)
	}
	{
		// Strings
		for _, colKind := range []string{"varchar", "char", "text", "nchar", "nvarchar", "ntext"} {
			dataType, opts, err := ParseColumnDataType(colKind, nil, nil, nil)
			assert.NoError(t, err, colKind)
			assert.Nil(t, opts, colKind)
			assert.Equal(t, String, dataType, colKind)
		}
	}
	{
		// Unique Identifier
		dataType, opts, err := ParseColumnDataType("uniqueidentifier", nil, nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, opts)
		assert.Equal(t, UniqueIdentifier, dataType)
	}
	{
		// Binary
		for _, colKind := range []string{"image", "binary", "varbinary"} {
			dataType, opts, err := ParseColumnDataType(colKind, nil, nil, nil)
			assert.NoError(t, err, colKind)
			assert.Nil(t, opts, colKind)
			assert.Equal(t, Bytes, dataType, colKind)
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
