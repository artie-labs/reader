package schema

import (
	"testing"

	ptr2 "github.com/artie-labs/reader/lib/ptr"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestParseColumnDataType(t *testing.T) {
	type _testCase struct {
		name      string
		colKind   string
		precision *int
		scale     *uint16
		udtName   *string

		expectedDataType DataType
		expectedOpts     *Opts
		expectedErr      string
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
			expectedDataType: TimeWithTimeZone,
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
			expectedDataType: Text,
		},
		{
			name:             "inet",
			colKind:          "inet",
			expectedDataType: Text,
		},
		{
			name:             "numeric",
			colKind:          "numeric",
			expectedDataType: VariableNumeric,
		},
		{
			name:             "numeric - with scale + precision",
			colKind:          "numeric",
			scale:            ptr2.ToUint16(2),
			precision:        ptr.ToInt(3),
			expectedDataType: Numeric,
			expectedOpts: &Opts{
				Scale:     2,
				Precision: 3,
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
		{
			name:        "unsupported",
			colKind:     "foo",
			expectedErr: `unknown data type: "foo"`,
		},
	}

	for _, testCase := range testCases {
		dataType, opts, err := ParseColumnDataType(testCase.colKind, testCase.precision, testCase.scale, testCase.udtName)
		if testCase.expectedErr == "" {
			assert.NoError(t, err, testCase.name)
			assert.Equal(t, testCase.expectedDataType, dataType, testCase.name)
			assert.Equal(t, testCase.expectedOpts, opts, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
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
