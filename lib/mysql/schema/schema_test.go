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
	testCases := []struct {
		input        string
		expectedType DataType
		expectedOpts *Opts
		expectedErr  string
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
			expectedOpts: &Opts{
				Precision: ptr.ToInt(5),
				Scale:     ptr2.ToUint16(2),
			},
		},
		{
			input:        "int(10) unsigned",
			expectedType: BigInt,
			expectedOpts: nil,
		},
		{
			input:        "tinyint unsigned",
			expectedType: SmallInt,
			expectedOpts: nil,
		},
		{
			input:        "smallint unsigned",
			expectedType: Int,
			expectedOpts: nil,
		},
		{
			input:        "mediumint unsigned",
			expectedType: Int,
			expectedOpts: nil,
		},
		{
			input:        "int unsigned",
			expectedType: BigInt,
			expectedOpts: nil,
		},
		{
			input:       "int(10 unsigned",
			expectedErr: `malformed data type: "int(10 unsigned"`,
		},
		{
			input:       "foo",
			expectedErr: `unknown data type: "foo"`,
		},
		{
			input:       "varchar(",
			expectedErr: `malformed data type: "varchar("`,
		},
	}

	for _, testCase := range testCases {
		colType, opts, err := parseColumnDataType(testCase.input)
		if testCase.expectedErr == "" {
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedType, colType, testCase.input)
			assert.Equal(t, testCase.expectedOpts, opts, testCase.input)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.input)
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
