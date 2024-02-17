package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/primary_key"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/transfer/lib/ptr"
)

func TestShouldQuoteValue(t *testing.T) {
	tests := []struct {
		name     string
		col      schema.Column
		value    string
		expected bool
	}{
		{"InvalidDataType", schema.Column{Type: schema.InvalidDataType}, "invalid", true},
		{"VariableNumeric", schema.Column{Type: schema.VariableNumeric}, "var_numeric", true},
		{"Money", schema.Column{Type: schema.Money}, "1.00", true},
		{"Numeric", schema.Column{Type: schema.Numeric}, "1.23", true},
		{"Bit", schema.Column{Type: schema.Bit}, "1", false},
		{"Boolean", schema.Column{Type: schema.Boolean}, "true", false},
		{"TextThatRequiresEscaping", schema.Column{Type: schema.TextThatRequiresEscaping}, "select", true},
		{"Text", schema.Column{Type: schema.Text}, "foo", true},
		{"Interval", schema.Column{Type: schema.Interval}, "", false},
		{"Array", schema.Column{Type: schema.Array}, "", false},
		{"HStore", schema.Column{Type: schema.HStore}, "", true},
		{"Float", schema.Column{Type: schema.Float}, "12.34", false},
		{"Int16", schema.Column{Type: schema.Int16}, "12", false},
		{"Int32", schema.Column{Type: schema.Int32}, "12", false},
		{"Int64", schema.Column{Type: schema.Int64}, "12", false},
		{"UUID", schema.Column{Type: schema.UUID}, "", true},
		{"UserDefinedText", schema.Column{Type: schema.UserDefinedText}, "foo", true},
		{"JSON", schema.Column{Type: schema.JSON}, "{}", true},
		{"Timestamp", schema.Column{Type: schema.Timestamp}, "2000-01-02 03:04:05", true},
		{"Time", schema.Column{Type: schema.Time}, "03:04:05", true},
		{"Date", schema.Column{Type: schema.Date}, "2000-01-02", true},
		// PostGIS
		{"Point", schema.Column{Type: schema.Point}, "", true},
		{"Geometry", schema.Column{Type: schema.Geometry}, "", true},
		{"Geography", schema.Column{Type: schema.Geography}, "", true},
	}

	for _, tc := range tests {
		tc.col.Name = tc.name
		assert.Equal(t, tc.expected, shouldQuoteValue(tc.col, tc.value), tc.name)
	}
}

func TestKeysToValueList(t *testing.T) {
	primaryKeys := primary_key.NewKeys()
	primaryKeys.Upsert("a", ptr.ToString("1"), ptr.ToString("4"))
	primaryKeys.Upsert("b", ptr.ToString("a"), ptr.ToString("z"))
	primaryKeys.Upsert("c", ptr.ToString("2000-01-02 03:04:05"), ptr.ToString("2001-01-02 03:04:05"))
	cols := []schema.Column{
		{Name: "a", Type: schema.Int64},
		{Name: "b", Type: schema.Text},
		{Name: "c", Type: schema.Timestamp},
	}

	{
		values, err := keysToValueList(primaryKeys, cols, false)
		assert.NoError(t, err)
		assert.Equal(t, []string{"1", "'a'", "'2000-01-02 03:04:05'"}, values)
	}
	{
		values, err := keysToValueList(primaryKeys, cols, true)
		assert.NoError(t, err)
		assert.Equal(t, []string{"4", "'z'", "'2001-01-02 03:04:05'"}, values)
	}
}

func TestScanTableQuery(t *testing.T) {
	primaryKeys := primary_key.NewKeys()
	primaryKeys.Upsert("a", ptr.ToString("1"), ptr.ToString("4"))
	primaryKeys.Upsert("b", ptr.ToString("2"), ptr.ToString("5"))
	primaryKeys.Upsert("c", ptr.ToString("3"), ptr.ToString("6"))

	query, err := scanTableQuery(scanTableQueryArgs{
		Schema:      "schema",
		TableName:   "table",
		PrimaryKeys: primaryKeys,
		FirstWhere:  GreaterThanEqualTo,
		SecondWhere: GreaterThan,
		Limit:       1,
		Columns: []schema.Column{
			{Name: "a", Type: schema.Int64},
			{Name: "b", Type: schema.Int64},
			{Name: "c", Type: schema.Int64},
			{Name: "e", Type: schema.Text},
			{Name: "f", Type: schema.Int64},
			{Name: "g", Type: schema.Money}, // money will be cast
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, `SELECT "a","b","c","e","f","g"::text FROM "schema"."table" WHERE row("a","b","c") >= row(1,2,3) AND NOT row("a","b","c") > row(4,5,6) ORDER BY "a","b","c" LIMIT 1`, query)
}
