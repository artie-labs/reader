package scanner

import (
	"testing"
	"time"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/stretchr/testify/assert"
)

func TestScanAdapter_ParsePrimaryKeyValue(t *testing.T) {
	{
		// Column does not exist
		adapter := scanAdapter{columns: []schema.Column{{Name: "bar"}}}
		_, err := adapter.ParsePrimaryKeyValue("foo", "1234")
		assert.ErrorContains(t, err, "primary key column does not exist: foo")
	}

	testCases := []struct {
		name        string
		dataType    schema.DataType
		value       string
		expected    any
		expectedErr string
	}{
		{
			name:        "unsupported data type",
			dataType:    0,
			value:       "1234",
			expectedErr: "primary key value parsing not implemented for DataType",
		},
		{
			name:        "unsupported binary type",
			dataType:    schema.Blob,
			value:       "1234",
			expectedErr: "primary key value parsing not implemented for binary types",
		},
		{
			name:        "tinyint - malformed",
			dataType:    schema.TinyInt,
			value:       "asdf",
			expectedErr: `unable to convert "asdf" to a tinyint: strconv.ParseInt: parsing "asdf": invalid syntax`,
		},
		{
			name:        "tinyint - out of range",
			dataType:    schema.TinyInt,
			value:       "1234",
			expectedErr: `unable to convert "1234" to a tinyint: strconv.ParseInt: parsing "1234": value out of range`,
		},
		{
			name:     "tinyint - well-formed",
			dataType: schema.TinyInt,
			value:    "40",
			expected: int8(40),
		},
		{
			name:        "smallint - malformed",
			dataType:    schema.SmallInt,
			value:       "asdf",
			expectedErr: `unable to convert "asdf" to a smallint: strconv.ParseInt: parsing "asdf": invalid syntax`,
		},
		{
			name:        "smallint - out of range",
			dataType:    schema.SmallInt,
			value:       "32768",
			expectedErr: `unable to convert "32768" to a smallint: strconv.ParseInt: parsing "32768": value out of range`,
		},
		{
			name:     "smallint - well-formed",
			dataType: schema.SmallInt,
			value:    "32767",
			expected: int16(32767),
		},
		{
			name:        "mediumint - malformed",
			dataType:    schema.MediumInt,
			value:       "asdf",
			expectedErr: `unable to convert "asdf" to a mediumint: strconv.ParseInt: parsing "asdf": invalid syntax`,
		},
		{
			name:        "mediumint - out of range",
			dataType:    schema.MediumInt,
			value:       "8388608",
			expectedErr: `unable to convert "8388608" to a mediumint: strconv.ParseInt: parsing "8388608": value out of range`,
		},
		{
			name:     "mediumint - well-formed",
			dataType: schema.MediumInt,
			value:    "8388607",
			expected: int32(8388607),
		},
		{
			name:        "int - malformed",
			dataType:    schema.Int,
			value:       "asdf",
			expectedErr: `unable to convert "asdf" to an int: strconv.ParseInt: parsing "asdf": invalid syntax`,
		},
		{
			name:        "int - out of range",
			dataType:    schema.Int,
			value:       "2147483648",
			expectedErr: `unable to convert "2147483648" to an int: strconv.ParseInt: parsing "2147483648": value out of range`,
		},
		{
			name:     "int - well-formed",
			dataType: schema.Int,
			value:    "2147483646",
			expected: int32(2147483646),
		},
		{
			name:        "bigint - malformed",
			dataType:    schema.BigInt,
			value:       "asdf",
			expectedErr: `unable to convert "asdf" to a bigint: strconv.ParseInt: parsing "asdf": invalid syntax`,
		},
		{
			name:        "bigint - out of range",
			dataType:    schema.BigInt,
			value:       "9223372036854775808",
			expectedErr: `unable to convert "9223372036854775808" to a bigint: strconv.ParseInt: parsing "9223372036854775808": value out of range`,
		},
		{
			name:     "bigint - well-formed",
			dataType: schema.BigInt,
			value:    "9223372036854775806",
			expected: int64(9223372036854775806),
		},
		{
			name:        "float - malformed",
			dataType:    schema.Float,
			value:       "asdf",
			expectedErr: `unable to convert "asdf" to a float: strconv.ParseFloat: parsing "asdf": invalid syntax`,
		},
		{
			name:     "float - well-formed",
			dataType: schema.Float,
			value:    "123445.79",
			expected: float32(12_3445.79),
		},
		{
			name:        "double - malformed",
			dataType:    schema.Double,
			value:       "asdf",
			expectedErr: `unable to convert "asdf" to a double: strconv.ParseFloat: parsing "asdf": invalid syntax`,
		},
		{
			name:     "double - well-formed",
			dataType: schema.Double,
			value:    "3.141592653589793",
			expected: float64(3.141592653589793),
		},
		{
			name:        "timestamp - malformed",
			dataType:    schema.Timestamp,
			value:       "jan 12",
			expectedErr: `parsing time "jan 12" as "2006-01-02 15:04:05.999999999": cannot parse "jan 12" as "2006"`,
		},
		{
			name:     "timestamp - well-formed",
			dataType: schema.Timestamp,
			value:    "2001-02-03 04:05:06.1234",
			expected: time.Date(2001, 2, 3, 4, 5, 6, 123400000, time.UTC),
		},
		{
			name:        "datetime - malformed",
			dataType:    schema.DateTime,
			value:       "jan 12",
			expectedErr: `parsing time "jan 12" as "2006-01-02 15:04:05.999999999": cannot parse "jan 12" as "2006"`,
		},
		{
			name:     "datetime - well-formed",
			dataType: schema.DateTime,
			value:    "2001-02-03 04:05:06.1234",
			expected: time.Date(2001, 2, 3, 4, 5, 6, 123400000, time.UTC),
		},
		{
			name:        "year - malformed",
			dataType:    schema.Year,
			value:       "asdf",
			expectedErr: `unable to convert "asdf" to a year: strconv.ParseInt: parsing "asdf": invalid syntax`,
		},
		{
			name:        "year - too large",
			dataType:    schema.Year,
			value:       "2156",
			expectedErr: `unable to convert "2156" to a year: value must be <= 2155`,
		},
		{
			name:     "year - just large enough",
			dataType: schema.Year,
			value:    "2155",
			expected: int16(2155),
		},
		{
			name:        "year - too small",
			dataType:    schema.Year,
			value:       "1900",
			expectedErr: `unable to convert "1900" to a year: value must be >= 1901`,
		},
		{
			name:     "year - just small enough",
			dataType: schema.Year,
			value:    "1901",
			expected: int16(1901),
		},
		{
			name:     "year - well-formed",
			dataType: schema.Year,
			value:    "2002",
			expected: int16(2002),
		},
		{
			name:     "text",
			dataType: schema.Text,
			value:    "red orange yellow",
			expected: "red orange yellow",
		},
	}

	for _, testCase := range testCases {
		adapter := scanAdapter{columns: []schema.Column{{Name: "col", Type: testCase.dataType}}}
		value, err := adapter.ParsePrimaryKeyValue("col", testCase.value)
		if testCase.expectedErr == "" {
			assert.NoError(t, err, testCase.name)
			assert.Equal(t, testCase.expected, value, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}
