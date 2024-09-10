package schema

import (
	"testing"

	ptr2 "github.com/artie-labs/reader/lib/ptr"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestQuoteIdentifier(t *testing.T) {
	assert.Equal(t, "`foo`", QuoteIdentifier("foo"))
	assert.Equal(t, "`fo``o`", QuoteIdentifier("fo`o"))
}

func TestParseColumnDataType(t *testing.T) {
	{
		// Invalid
		{
			_, _, err := parseColumnDataType("int(10 unsigned")
			assert.ErrorContains(t, err, `malformed data type: "int(10 unsigned"`)
		}
		{
			_, _, err := parseColumnDataType("foo")
			assert.ErrorContains(t, err, `unknown data type: "foo"`)
		}
		{
			_, _, err := parseColumnDataType("varchar(")
			assert.ErrorContains(t, err, `malformed data type: "varchar("`)
		}
	}
	{
		// Integers
		{
			// int
			dataType, _, err := parseColumnDataType("int")
			assert.NoError(t, err)
			assert.Equal(t, Int, dataType)
		}
		{
			// int unsigned
			dataType, _, err := parseColumnDataType("int unsigned")
			assert.NoError(t, err)
			assert.Equal(t, BigInt, dataType)
		}
		{
			// int(10) unsigned
			dataType, _, err := parseColumnDataType("int(10) unsigned")
			assert.NoError(t, err)
			assert.Equal(t, BigInt, dataType)
		}
		{
			// tinyint
			dataType, _, err := parseColumnDataType("tinyint")
			assert.NoError(t, err)
			assert.Equal(t, TinyInt, dataType)
		}
		{
			// tinyint unsigned
			dataType, _, err := parseColumnDataType("tinyint unsigned")
			assert.NoError(t, err)
			assert.Equal(t, SmallInt, dataType)
		}
		{
			// mediumint unsigned
			dataType, _, err := parseColumnDataType("mediumint unsigned")
			assert.NoError(t, err)
			assert.Equal(t, Int, dataType)
		}
	}
	{
		// tinyint(1) or boolean
		dataType, _, err := parseColumnDataType("tinyint(1)")
		assert.NoError(t, err)
		assert.Equal(t, Boolean, dataType)
	}
	{
		// String
		dataType, opts, err := parseColumnDataType("varchar(255)")
		assert.NoError(t, err)
		assert.Equal(t, Varchar, dataType)
		assert.Equal(t, &Opts{Size: ptr.ToInt(255)}, opts)
	}
	{
		// Decimal
		dataType, opts, err := parseColumnDataType("decimal(5,2)")
		assert.NoError(t, err)
		assert.Equal(t, Decimal, dataType)
		assert.Equal(t, &Opts{Precision: ptr.ToInt(5), Scale: ptr2.ToUint16(2)}, opts)
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
