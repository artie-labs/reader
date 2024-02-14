package schema

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestDescribeTableQuery(t *testing.T) {
	{
		query, args := DescribeTableQuery(DescribeTableArgs{
			Name:   "name",
			Schema: "schema",
		})
		assert.Equal(t, "SELECT column_name, data_type, numeric_precision, numeric_scale, udt_name\nFROM information_schema.columns\nWHERE table_name = $1 AND table_schema = $2", query)
		assert.Equal(t, []any{"name", "schema"}, args)
	}
	// test quotes in table name or schema are left alone
	{
		_, args := DescribeTableQuery(DescribeTableArgs{
			Name:   `na"me`,
			Schema: `s'ch"em'a`,
		})
		assert.Equal(t, []any{`na"me`, `s'ch"em'a`}, args)
	}
}

func TestColKindToDataType(t *testing.T) {
	type _testCase struct {
		name      string
		colKind   string
		precision *string
		scale     *string
		udtName   *string

		expectedDataType DataType
		expectedOpts     *Opts
	}

	var testCases = []_testCase{
		{
			name:             "array",
			colKind:          "ARRAY",
			expectedDataType: Array,
		},
		{
			name:             "character varying",
			colKind:          "character varying",
			expectedDataType: Text,
		},
		{
			name:             "bit",
			colKind:          "bit",
			expectedDataType: Bit,
		},
		{
			name:             "bool",
			colKind:          "boolean",
			expectedDataType: Boolean,
		},
		{
			name:             "interval",
			colKind:          "interval",
			expectedDataType: Interval,
		},
		{
			name:             "time with time zone",
			colKind:          "time with time zone",
			expectedDataType: Time,
		},
		{
			name:             "time without time zone",
			colKind:          "time without time zone",
			expectedDataType: Time,
		},
		{
			name:             "date",
			colKind:          "date",
			expectedDataType: Date,
		},
		{
			name:             "char_text",
			colKind:          "character",
			expectedDataType: TextThatRequiresEscaping,
		},
		{
			name:             "numeric",
			colKind:          "numeric",
			expectedDataType: VariableNumeric,
		},
		{
			name:             "numeric - with scale + precision",
			colKind:          "numeric",
			scale:            ptr.ToString("2"),
			precision:        ptr.ToString("3"),
			expectedDataType: Numeric,
			expectedOpts: &Opts{
				Scale:     ptr.ToString("2"),
				Precision: ptr.ToString("3"),
			},
		},
		{
			name:             "variable numeric",
			colKind:          "variable numeric",
			expectedDataType: VariableNumeric,
		},
		{
			name:             "money",
			colKind:          "money",
			expectedDataType: Money,
			expectedOpts: &Opts{
				Scale: ptr.ToString("2"), // money always has a scale of 2
			},
		},
		{
			name:             "hstore",
			colKind:          "user-defined",
			udtName:          ptr.ToString("hstore"),
			expectedDataType: HStore,
		},
		{
			name:             "geometry",
			colKind:          "user-defined",
			udtName:          ptr.ToString("geometry"),
			expectedDataType: Geometry,
		},
		{
			name:             "geography",
			colKind:          "user-defined",
			udtName:          ptr.ToString("geography"),
			expectedDataType: Geography,
		},
		{
			name:             "user-defined text",
			colKind:          "user-defined",
			udtName:          ptr.ToString("foo"),
			expectedDataType: UserDefinedText,
		},
	}

	for _, testCase := range testCases {
		dataType, opts := ColKindToDataType(testCase.colKind, testCase.precision, testCase.scale, testCase.udtName)
		assert.Equal(t, testCase.expectedDataType, dataType, testCase.name)
		assert.Equal(t, testCase.expectedOpts, opts, testCase.name)
	}
}
