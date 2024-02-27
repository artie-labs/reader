package schema

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestQuoteIdentifier(t *testing.T) {
	assert.Equal(t, "`foo`", QuoteIdentifier("foo"))
	assert.Equal(t, "`fo``o`", QuoteIdentifier("fo`o"))
}

func TestQuotedIdentifiers(t *testing.T) {
	assert.Equal(t, []string{"`fo``o`", "`a`", "`b`"}, QuotedIdentifiers([]string{"fo`o", "a", "b"}))
}

func TestParseColumnDataType(t *testing.T) {
	tests := []struct {
		input        string
		expectedType DataType
		expectedOpts *Opts
	}{
		{
			input:        "int",
			expectedType: Int,
		},
		{
			input:        "tinyint(1)",
			expectedType: Boolean,
		},
		{
			input:        "varchar(255)",
			expectedType: Varchar,
			expectedOpts: &Opts{Size: ptr.ToInt(255)},
		},
		{
			input:        "decimal(5,2)",
			expectedType: Decimal,
			expectedOpts: &Opts{Precision: ptr.ToInt(5), Scale: ptr.ToInt(2)},
		},
		{
			input:        "foo",
			expectedType: InvalidDataType,
		},
	}

	for _, test := range tests {
		colType, opts, err := parseColumnDataType(test.input)
		assert.NoError(t, err)
		assert.Equal(t, test.expectedType, colType, test.input)
		assert.Equal(t, test.expectedOpts, opts, test.input)
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
