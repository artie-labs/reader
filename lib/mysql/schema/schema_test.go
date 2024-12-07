package schema

import (
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	assert.Equal(t, "`foo`", QuoteIdentifier("foo"))
	assert.Equal(t, "`fo``o`", QuoteIdentifier("fo`o"))
}

func TestParseColumnDataType(t *testing.T) {
	{
		// Invalid
		{
			_, _, err := ParseColumnDataType("int(10 unsigned")
			assert.ErrorContains(t, err, `malformed data type: "int(10 unsigned"`)
		}
		{
			_, _, err := ParseColumnDataType("foo")
			assert.ErrorContains(t, err, `unknown data type: "foo"`)
		}
		{
			_, _, err := ParseColumnDataType("varchar(")
			assert.ErrorContains(t, err, `malformed data type: "varchar("`)
		}
	}
	{
		// Integers
		{
			// int
			dataType, _, err := ParseColumnDataType("int")
			assert.NoError(t, err)
			assert.Equal(t, Int, dataType)
		}
		{
			// int
			dataType, _, err := ParseColumnDataType("INTEGER")
			assert.NoError(t, err)
			assert.Equal(t, Int, dataType)
		}
		{
			// int unsigned
			dataType, _, err := ParseColumnDataType("int unsigned")
			assert.NoError(t, err)
			assert.Equal(t, BigInt, dataType)
		}
		{
			// int(10) unsigned
			dataType, _, err := ParseColumnDataType("int(10) unsigned")
			assert.NoError(t, err)
			assert.Equal(t, BigInt, dataType)
		}
		{
			// tinyint
			dataType, _, err := ParseColumnDataType("tinyint")
			assert.NoError(t, err)
			assert.Equal(t, TinyInt, dataType)
		}
		{
			// tinyint unsigned
			dataType, _, err := ParseColumnDataType("tinyint unsigned")
			assert.NoError(t, err)
			assert.Equal(t, SmallInt, dataType)
		}
		{
			// mediumint unsigned
			dataType, _, err := ParseColumnDataType("mediumint unsigned")
			assert.NoError(t, err)
			assert.Equal(t, Int, dataType)
		}
	}
	{
		// tinyint(1) should still be an integer
		dataType, _, err := ParseColumnDataType("tinyint(1)")
		assert.NoError(t, err)
		assert.Equal(t, TinyInt, dataType)
	}
	{
		// String
		dataType, opts, err := ParseColumnDataType("varchar(255)")
		assert.NoError(t, err)
		assert.Equal(t, Varchar, dataType)
		assert.Equal(t, &Opts{Size: typing.ToPtr(255)}, opts)
	}
	{
		// Decimal
		dataType, opts, err := ParseColumnDataType("decimal(5,2)")
		assert.NoError(t, err)
		assert.Equal(t, Decimal, dataType)
		assert.Equal(t, &Opts{Precision: typing.ToPtr(5), Scale: typing.ToPtr(uint16(2))}, opts)
	}
	{
		// ENUM
		dataType, opts, err := ParseColumnDataType("enum('a','b','c')")
		assert.NoError(t, err)
		assert.Equal(t, Enum, dataType)
		assert.Equal(t, &Opts{EnumValues: []string{"a", "b", "c"}}, opts)
	}
	{
		// ENUM (With special characters)
		dataType, opts, err := ParseColumnDataType("ENUM('active','inactive','on hold','approved by ''manager''','needs \\\\review')")
		assert.NoError(t, err)
		assert.Equal(t, Enum, dataType)
		assert.Equal(t, &Opts{EnumValues: []string{"active", "inactive", "on hold", "approved by 'manager'", "needs \\review"}}, opts)
	}
	{
		// Blob
		for _, blob := range []string{"blob", "tinyblob", "mediumblob", "longblob"} {
			dataType, _, err := ParseColumnDataType(blob)
			assert.NoError(t, err)
			assert.Equal(t, Blob, dataType, blob)
		}
	}
}

func TestBuildPkValuesQuery(t *testing.T) {
	keys := []Column{
		{Name: "a", Type: Int, Opts: nil},
		{Name: "b", Type: Int, Opts: nil},
		{Name: "c", Type: Int, Opts: nil},
	}

	{
		query := buildPkValuesQuery(keys, "my-table", true)
		assert.Equal(t, "SELECT `a`,`b`,`c` FROM `my-table` ORDER BY `a` DESC,`b` DESC,`c` DESC LIMIT 1", query)
	}
	{
		query := buildPkValuesQuery(keys, "my-table", false)
		assert.Equal(t, "SELECT `a`,`b`,`c` FROM `my-table` ORDER BY `a`,`b`,`c` LIMIT 1", query)
	}
}
