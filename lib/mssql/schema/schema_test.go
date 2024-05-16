package schema

import (
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
