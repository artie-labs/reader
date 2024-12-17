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
			_, _, err := ParseColumnDataType("int(10 unsigned", nil)
			assert.ErrorContains(t, err, `malformed data type: "int(10 unsigned"`)
		}
		{
			_, _, err := ParseColumnDataType("foo", nil)
			assert.ErrorContains(t, err, `unknown data type: "foo"`)
		}
		{
			_, _, err := ParseColumnDataType("varchar(", nil)
			assert.ErrorContains(t, err, `malformed data type: "varchar("`)
		}
	}
	{
		// Integers
		{
			// int
			dataType, _, err := ParseColumnDataType("int", nil)
			assert.NoError(t, err)
			assert.Equal(t, Int, dataType)
		}
		{
			// int
			dataType, _, err := ParseColumnDataType("INTEGER", nil)
			assert.NoError(t, err)
			assert.Equal(t, Int, dataType)
		}
		{
			// int unsigned
			dataType, _, err := ParseColumnDataType("int unsigned", nil)
			assert.NoError(t, err)
			assert.Equal(t, BigInt, dataType)
		}
		{
			// int(10) unsigned
			dataType, _, err := ParseColumnDataType("int(10) unsigned", nil)
			assert.NoError(t, err)
			assert.Equal(t, BigInt, dataType)
		}
		{
			// tinyint
			dataType, _, err := ParseColumnDataType("tinyint", nil)
			assert.NoError(t, err)
			assert.Equal(t, TinyInt, dataType)
		}
		{
			// tinyint unsigned
			dataType, _, err := ParseColumnDataType("tinyint unsigned", nil)
			assert.NoError(t, err)
			assert.Equal(t, SmallInt, dataType)
		}
		{
			// mediumint unsigned
			dataType, _, err := ParseColumnDataType("mediumint unsigned", nil)
			assert.NoError(t, err)
			assert.Equal(t, Int, dataType)
		}
	}
	{
		// tinyint(1) should still be an integer
		dataType, _, err := ParseColumnDataType("tinyint(1)", nil)
		assert.NoError(t, err)
		assert.Equal(t, TinyInt, dataType)
	}
	{
		// String
		{
			// VARCHAR
			dataType, opts, err := ParseColumnDataType("varchar(255)", nil)
			assert.NoError(t, err)
			assert.Equal(t, Varchar, dataType)
			assert.Equal(t, &Opts{Size: typing.ToPtr(255)}, opts)
		}
		{
			// VARCHAR with collation
			dataType, opts, err := ParseColumnDataType(`varchar(255) COLLATE utf8mb3_unicode_ci`, nil)
			assert.NoError(t, err)
			assert.Equal(t, Varchar, dataType)
			assert.Equal(t, &Opts{Size: typing.ToPtr(255)}, opts)
		}
		{
			// VARCHAR with collation and character set
			dataType, opts, err := ParseColumnDataType(`varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci`, nil)
			assert.NoError(t, err)
			assert.Equal(t, Varchar, dataType)
			assert.Equal(t, &Opts{Size: typing.ToPtr(255)}, opts)
		}
	}
	{
		// Decimal
		dataType, opts, err := ParseColumnDataType("decimal(5,2)", nil)
		assert.NoError(t, err)
		assert.Equal(t, Decimal, dataType)
		assert.Equal(t, &Opts{Precision: typing.ToPtr(5), Scale: typing.ToPtr(uint16(2))}, opts)
	}
	{
		// Enum
		{
			// No need to escape
			dataType, opts, err := ParseColumnDataType("enum('a','b','c')", nil)
			assert.NoError(t, err)
			assert.Equal(t, Enum, dataType)
			assert.Equal(t, &Opts{EnumValues: []string{"a", "b", "c"}}, opts)
		}
		{
			// No need to escape, testing for capitalization
			dataType, opts, err := ParseColumnDataType("ENUM('A','B','C')", nil)
			assert.NoError(t, err)
			assert.Equal(t, Enum, dataType)
			assert.Equal(t, &Opts{EnumValues: []string{"A", "B", "C"}}, opts)
		}
		{
			// Need to escape
			dataType, opts, err := ParseColumnDataType(`enum('newline\n','tab	','backslash\\','quote''s')`, nil)
			assert.NoError(t, err)
			assert.Equal(t, Enum, dataType)
			assert.Equal(t, &Opts{EnumValues: []string{"newline\\n", "tab\t", "backslash\\\\", "quote's"}}, opts)
			assert.Equal(t, &Opts{EnumValues: []string{"newline\\n", `tab	`, `backslash\\`, "quote's"}}, opts)

		}
		{
			// Need to escape another one
			dataType, opts, err := ParseColumnDataType("ENUM('active','inactive','on hold','approved by ''manager''','needs \\\\review')", nil)
			assert.NoError(t, err)
			assert.Equal(t, Enum, dataType)
			assert.Equal(t, &Opts{EnumValues: []string{"active", "inactive", "on hold", "approved by 'manager'", "needs \\\\review"}}, opts)
			assert.Equal(t, &Opts{EnumValues: []string{"active", "inactive", "on hold", `approved by 'manager'`, `needs \\review`}}, opts)
		}
	}
	{
		// Set
		{
			// No need to escape
			dataType, opts, err := ParseColumnDataType("set('a','b','c')", nil)
			assert.NoError(t, err)
			assert.Equal(t, Set, dataType)
			assert.Equal(t, &Opts{EnumValues: []string{"a", "b", "c"}}, opts)
		}
		{
			// No need to escape, testing for capitalization
			dataType, opts, err := ParseColumnDataType("SET('A','B','C')", nil)
			assert.NoError(t, err)
			assert.Equal(t, Set, dataType)
			assert.Equal(t, &Opts{EnumValues: []string{"A", "B", "C"}}, opts)
		}
	}
	{
		// Blob
		for _, blob := range []string{"blob", "tinyblob", "mediumblob", "longblob"} {
			dataType, _, err := ParseColumnDataType(blob, nil)
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
