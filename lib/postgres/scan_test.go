package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

func TestShouldQuoteValue(t *testing.T) {
	tests := []struct {
		name     string
		dataType schema.DataType
		expected bool
	}{
		{"VariableNumeric", schema.VariableNumeric, true},
		{"Money", schema.Money, true},
		{"Numeric", schema.Numeric, true},
		{"Bit", schema.Bit, false},
		{"Boolean", schema.Boolean, false},
		{"TextThatRequiresEscaping", schema.TextThatRequiresEscaping, true},
		{"Text", schema.Text, true},
		{"Interval", schema.Interval, false},
		{"Array", schema.Array, false},
		{"HStore", schema.HStore, true},
		{"Float", schema.Float, false},
		{"Int16", schema.Int16, false},
		{"Int32", schema.Int32, false},
		{"Int64", schema.Int64, false},
		{"UUID", schema.UUID, true},
		{"UserDefinedText", schema.UserDefinedText, true},
		{"JSON", schema.JSON, true},
		{"Timestamp", schema.Timestamp, true},
		{"Time", schema.Time, true},
		{"Date", schema.Date, true},
		// PostGIS
		{"Point", schema.Point, true},
		{"Geometry", schema.Geometry, true},
		{"Geography", schema.Geography, true},
	}

	for _, tc := range tests {
		result, err := shouldQuoteValue(tc.dataType)
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, result, tc.name)
	}

	_, err := shouldQuoteValue(schema.InvalidDataType)
	assert.ErrorContains(t, err, "invalid data type")
}

func TestKeysToValueList(t *testing.T) {
	primaryKeys := []primary_key.Key{
		{Name: "a", StartingValue: "1", EndingValue: "4"},
		{Name: "b", StartingValue: "a", EndingValue: "z"},
		{Name: "c", StartingValue: "2000-01-02 03:04:05", EndingValue: "2001-01-02 03:04:05"},
	}

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
	{
		primaryKeys := append(primaryKeys, primary_key.Key{Name: "d", StartingValue: "1", EndingValue: "4"})
		_, err := keysToValueList(primaryKeys, cols, true)
		assert.ErrorContains(t, err, "primary key d not found in columns")
	}
}

func TestScanTableQuery(t *testing.T) {
	primaryKeys := []primary_key.Key{
		{Name: "a", StartingValue: "1", EndingValue: "4"},
		{Name: "b", StartingValue: "2", EndingValue: "5"},
		{Name: "c", StartingValue: "3", EndingValue: "6"},
	}
	cols := []schema.Column{
		{Name: "a", Type: schema.Int64},
		{Name: "b", Type: schema.Int64},
		{Name: "c", Type: schema.Int64},
		{Name: "e", Type: schema.Text},
		{Name: "f", Type: schema.Int64},
		{Name: "g", Type: schema.TextThatRequiresEscaping}, // Requires casting
	}

	{
		// inclusive lower bound
		query, err := scanTableQuery(scanTableQueryArgs{
			Schema:              "schema",
			TableName:           "table",
			PrimaryKeys:         primaryKeys,
			InclusiveLowerBound: true,
			Limit:               1,
			Columns:             cols,
		})
		assert.NoError(t, err)
		assert.Equal(t, `SELECT "a","b","c","e","f","g"::text FROM "schema"."table" WHERE row("a","b","c") >= row(1,2,3) AND row("a","b","c") <= row(4,5,6) ORDER BY "a","b","c" LIMIT 1`, query)
	}
	{
		// exclusive lower bound
		query, err := scanTableQuery(scanTableQueryArgs{
			Schema:              "schema",
			TableName:           "table",
			PrimaryKeys:         primaryKeys,
			InclusiveLowerBound: false,
			Limit:               1,
			Columns:             cols,
		})
		assert.NoError(t, err)
		assert.Equal(t, `SELECT "a","b","c","e","f","g"::text FROM "schema"."table" WHERE row("a","b","c") > row(1,2,3) AND row("a","b","c") <= row(4,5,6) ORDER BY "a","b","c" LIMIT 1`, query)
	}
}
