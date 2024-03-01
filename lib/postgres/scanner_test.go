package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

func TestShouldQuoteValue(t *testing.T) {
	testCases := []struct {
		name        string
		dataType    schema.DataType
		expected    bool
		expectedErr string
	}{
		{"VariableNumeric", schema.VariableNumeric, true, ""},
		{"Money", schema.Money, true, ""},
		{"Numeric", schema.Numeric, true, ""},
		{"Bit", schema.Bit, false, "unsupported primary key type: DataType"},
		{"Boolean", schema.Boolean, false, ""},
		{"Inet", schema.Inet, true, ""},
		{"Text", schema.Text, true, ""},
		{"Interval", schema.Interval, false, "unsupported primary key type: DataType"},
		{"Array", schema.Array, false, "unsupported primary key type: DataType"},
		{"HStore", schema.HStore, true, "unsupported primary key type: DataType"},
		{"Float", schema.Float, false, ""},
		{"Int16", schema.Int16, false, ""},
		{"Int32", schema.Int32, false, ""},
		{"Int64", schema.Int64, false, ""},
		{"UUID", schema.UUID, true, ""},
		{"UserDefinedText", schema.UserDefinedText, true, ""},
		{"JSON", schema.JSON, true, ""},
		{"Timestamp", schema.Timestamp, true, ""},
		{"Time", schema.Time, true, "unsupported primary key type: DataType"},
		{"Date", schema.Date, true, ""},
		// PostGIS
		{"Point", schema.Point, true, "unsupported primary key type: DataType"},
		{"Geometry", schema.Geometry, true, "unsupported primary key type: DataType"},
		{"Geography", schema.Geography, true, "unsupported primary key type: DataType"},
	}

	for _, tc := range testCases {
		actual, err := shouldQuoteValue(tc.dataType)
		if tc.expectedErr == "" {
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.expected, actual, tc.name)
		} else {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		}
	}

	_, err := shouldQuoteValue(schema.InvalidDataType)
	assert.ErrorContains(t, err, "invalid data type")
}

func TestConvertToStringForQuery(t *testing.T) {
	testCases := []struct {
		name        string
		dataType    schema.DataType
		value       any
		expected    any
		expectedErr string
	}{
		{
			name:     "time - schema.Int64",
			value:    time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC),
			dataType: schema.Int64, // isn't checked for time.Time
			expected: "'2001-02-03T04:05:06Z'",
		},
		{
			name:     "time - schema.Text",
			value:    time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC),
			dataType: schema.Text, // isn't checked for time.Time
			expected: "'2001-02-03T04:05:06Z'",
		},
		{
			name:     "int64",
			value:    int64(1234),
			dataType: schema.Int64,
			expected: "1234",
		},
		{
			name:     "float64",
			value:    float64(1234.1234),
			dataType: schema.Float,
			expected: "1234.1234",
		},
		{
			name:     "text",
			value:    "foo",
			dataType: schema.Text,
			expected: "'foo'",
		},
		{
			name:        "text",
			value:       "foo",
			dataType:    schema.InvalidDataType,
			expectedErr: "invalid data type",
		},
		{
			name:        "text",
			value:       "foo",
			dataType:    -1,
			expectedErr: "unsupported data type",
		},
	}
	for _, testCase := range testCases {
		actual, err := convertToStringForQuery(testCase.value, testCase.dataType)
		if testCase.expectedErr == "" {
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, actual, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
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
		{Name: "127.0.0.1", Type: schema.Inet},
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
		assert.Equal(t, `SELECT "a","b","c","e","f","127.0.0.1"::text FROM "schema"."table" WHERE row("a","b","c") >= row(1,2,3) AND row("a","b","c") <= row(4,5,6) ORDER BY "a","b","c" LIMIT 1`, query)
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
		assert.Equal(t, `SELECT "a","b","c","e","f","127.0.0.1"::text FROM "schema"."table" WHERE row("a","b","c") > row(1,2,3) AND row("a","b","c") <= row(4,5,6) ORDER BY "a","b","c" LIMIT 1`, query)
	}
}
