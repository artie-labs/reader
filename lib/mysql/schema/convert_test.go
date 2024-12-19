package schema

import (
	"encoding/base64"
	"math"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func mustDecodeBase64(value string) []byte {
	result, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return result
}

func TestConvertValue(t *testing.T) {
	{
		// TinyInt
		{
			// Malformed
			_, err := ConvertValue("bad tiny int", TinyInt, nil)
			assert.ErrorContains(t, err, "expected integers, got string with value: bad tiny int")
		}
		{
			// Int64
			value, err := ConvertValue(int64(100), TinyInt, nil)
			assert.NoError(t, err)
			assert.Equal(t, int64(100), value)
		}
	}
	{
		// Floats
		{
			// Invalid type
			_, err := ConvertValue("bad float", Float, nil)
			assert.ErrorContains(t, err, "expected float32 or float64 got string with value: bad float")
		}
		{
			// Float32
			value, err := ConvertValue(float32(1.234), Float, nil)
			assert.NoError(t, err)
			assert.Equal(t, float32(1.234), value)
		}
		{
			// Float64 (within range)
			value, err := ConvertValue(float64(1.234), Float, nil)
			assert.NoError(t, err)
			assert.Equal(t, float32(1.234), value)
		}
		{
			// Float64 (overflow)
			_, err := ConvertValue(float64(math.MaxFloat32*1.5), Float, nil)
			assert.ErrorContains(t, err, "value overflows float32")
		}
	}
	{
		// Set
		{
			// Passed in as a string
			value, err := ConvertValue("dogs,cats,mouse", Set, &Opts{EnumValues: []string{"dogs", "cats", "mouse"}})
			assert.NoError(t, err)
			assert.Equal(t, "dogs,cats,mouse", value)
		}
		{
			// Passed in as int64
			opts := &Opts{EnumValues: []string{"dogs", "cats", "mouse"}}
			{
				value, err := ConvertValue(int64(0), Set, opts)
				assert.NoError(t, err)
				assert.Equal(t, "", value)
			}
			{
				value, err := ConvertValue(int64(1), Set, opts)
				assert.NoError(t, err)
				assert.Equal(t, "dogs", value)
			}
			{
				value, err := ConvertValue(int64(2), Set, opts)
				assert.NoError(t, err)
				assert.Equal(t, "cats", value)
			}
			{
				value, err := ConvertValue(int64(5), Set, opts)
				assert.NoError(t, err)
				assert.Equal(t, "dogs,mouse", value)
			}
			{
				value, err := ConvertValue(int64(7), Set, opts)
				assert.NoError(t, err)
				assert.Equal(t, "dogs,cats,mouse", value)
			}
		}
	}
	{
		// Enum
		{
			// Passed in as a string
			value, err := ConvertValue("dogs", Enum, &Opts{EnumValues: []string{"dogs", "cats", "mouse"}})
			assert.NoError(t, err)
			assert.Equal(t, "dogs", value)
		}
		{
			// Passed in as int64
			opts := &Opts{EnumValues: []string{"dogs", "cats", "mouse"}}
			{
				// Valid
				value, err := ConvertValue(int64(0), Enum, opts)
				assert.NoError(t, err)
				assert.Equal(t, "dogs", value)
			}
			{
				// Invalid
				_, err := ConvertValue(int64(3), Enum, opts)
				assert.ErrorContains(t, err, "enum value 3 not in range [0, 2]")
			}
		}
	}

	tests := []struct {
		name        string
		dataType    DataType
		opts        *Opts
		value       any
		expected    any
		expectedErr string
	}{
		{
			name:     "nil",
			dataType: Varchar,
			value:    nil,
			expected: nil,
		},
		{
			name:     "bit - 0 value",
			dataType: Bit,
			value:    []byte{byte(0)},
			opts:     &Opts{Size: typing.ToPtr(1)},
			expected: false,
		},
		{
			name:     "bit - 1 value",
			dataType: Bit,
			value:    []byte{byte(1)},
			opts:     &Opts{Size: typing.ToPtr(1)},
			expected: true,
		},
		{
			name:        "bit - 2 value",
			dataType:    Bit,
			value:       []byte{byte(2)},
			opts:        &Opts{Size: typing.ToPtr(1)},
			expectedErr: "bit value is invalid",
		},
		{
			name:        "bit - 2 bytes",
			dataType:    Bit,
			value:       []byte{byte(1), byte(1)},
			opts:        &Opts{Size: typing.ToPtr(1)},
			expectedErr: "bit value is invalid",
		},
		{
			name:     "boolean - 0",
			dataType: Boolean,
			value:    int64(0),
			expected: false,
		},
		{
			name:     "boolean - 1",
			dataType: Boolean,
			value:    int64(1),
			expected: true,
		},
		{
			name:        "boolean - -2",
			dataType:    Boolean,
			value:       int64(-2),
			expectedErr: "boolean value -2 not in [0, 1]",
		},
		{
			name:        "boolean - 2",
			dataType:    Boolean,
			value:       int64(2),
			expectedErr: "boolean value 2 not in [0, 1]",
		},
		{
			name:     "small int",
			dataType: SmallInt,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:     "medium int",
			dataType: MediumInt,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:     "big int",
			dataType: BigInt,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:     "int",
			dataType: Int,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:     "year",
			dataType: Year,
			value:    int64(2021),
			expected: int64(2021),
		},
		{
			name:     "double",
			dataType: Double,
			value:    float64(1.234),
			expected: float64(1.234),
		},
		{
			name:     "binary",
			dataType: Binary,
			value:    []byte("hello world"),
			expected: []byte("hello world"),
		},
		{
			name:        "binary - malformed",
			dataType:    Binary,
			value:       true,
			expectedErr: "expected []byte or string got bool for value",
		},
		{
			name:     "varbinary",
			dataType: Varbinary,
			value:    []byte("hello world"),
			expected: []byte("hello world"),
		},
		{
			name:     "blob",
			dataType: Blob,
			value:    []byte("hello world"),
			expected: []byte("hello world"),
		},
		{
			name:     "decimal",
			dataType: Decimal,
			value:    []byte("1.234"),
			expected: "1.234",
		},
		{
			name:     "date",
			dataType: Date,
			value:    []byte("2021-01-02"),
			expected: "2021-01-02",
		},
		{
			name:     "date - 0000-00-00",
			dataType: Date,
			value:    []byte("0000-00-00"),
			expected: "0000-00-00",
		},
		{
			name:     "datetime (0000-00-00)",
			dataType: DateTime,
			value:    []byte("0000-00-00 00:00:00"),
			expected: nil,
		},
		{
			name:     "datetime",
			dataType: DateTime,
			value:    []byte("2021-01-02 03:04:05"),
			expected: time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		{
			name:        "datetime - malformed",
			dataType:    DateTime,
			value:       []byte("not a datetime"),
			expectedErr: `cannot parse "not a datetime" as "2006"`,
		},
		{
			name:     "timestamp",
			dataType: Timestamp,
			value:    []byte("2021-01-02 03:04:05"),
			expected: time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		{
			name:        "timestamp - malformed",
			dataType:    Timestamp,
			value:       []byte("not a timestamp"),
			expectedErr: `cannot parse "not a timestamp" as "2006"`,
		},
		{
			name:     "time",
			dataType: Time,
			value:    []byte("10:10:10"),
			expected: "10:10:10",
		},
		{
			name:     "char",
			dataType: Char,
			value:    []byte("X"),
			expected: "X",
		},
		{
			name:     "varchar",
			dataType: Varchar,
			value:    []byte("hello world"),
			expected: "hello world",
		},
		{
			name:        "varchar - malformed",
			dataType:    Varchar,
			value:       1234,
			expectedErr: "expected string or []byte got int for value: 1234",
		},
		{
			name:     "text",
			dataType: Text,
			value:    []byte("hello world"),
			expected: "hello world",
		},
		{
			name:     "json",
			dataType: JSON,
			value:    []byte(`{"foo": "bar", "baz": "1234"}`),
			expected: `{"foo": "bar", "baz": "1234"}`,
		},
		{
			name:     "point - zero values",
			dataType: Point,
			value:    mustDecodeBase64("AAAAAAEBAAAAAAAAAAAAAAAAAAAAAAAAAA=="),
			expected: map[string]any{"x": 0.0, "y": 0.0},
		},
		{
			name:     "point - positive values",
			dataType: Point,
			value:    mustDecodeBase64("AAAAAAEBAAAArkfhehSuKECkcD0K12NMQA=="),
			expected: map[string]any{"x": 12.34, "y": 56.78},
		},
		{
			name:     "point - negative values",
			dataType: Point,
			value:    mustDecodeBase64("AAAAAAEBAAAASOF6FK5IocDD9ShcjzmqwA=="),
			expected: map[string]any{"x": -2212.34, "y": -3356.78},
		},
	}

	for _, tc := range tests {
		value, err := ConvertValue(tc.value, tc.dataType, tc.opts)
		if tc.expectedErr == "" {
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.expected, value, tc.name)
		} else {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		}
	}
}

func TestConvertValues(t *testing.T) {
	columns := []Column{
		{Name: "a", Type: Int},
		{Name: "b", Type: Varchar},
		{Name: "c", Type: Bit, Opts: &Opts{Size: typing.ToPtr(1)}},
	}

	{
		// Too few values
		assert.ErrorContains(t, ConvertValues([]any{}, columns), "values and cols are not the same length")
	}
	{
		// Malformed data
		err := ConvertValues([]any{"bad", "bad", "bad"}, columns)
		assert.ErrorContains(t, err, `failed to convert value for column "a": expected integers, got string with value: bad`)
	}
	{
		// Happy path - nils
		values := []any{nil, nil, nil}
		assert.NoError(t, ConvertValues(values, columns))
		assert.Equal(t, []any{nil, nil, nil}, values)
	}
	{
		// Happy path - no nils
		values := []any{int64(1234), []byte("hello world"), []byte{byte(1)}}
		assert.NoError(t, ConvertValues(values, columns))
		assert.Equal(t, []any{int64(1234), "hello world", true}, values)
	}
}

func TestHasNonStrictModeInvalidDate(t *testing.T) {
	assert.False(t, hasNonStrictModeInvalidDate(""))
	assert.False(t, hasNonStrictModeInvalidDate("hello world"))
	assert.False(t, hasNonStrictModeInvalidDate("2021-01-02"))
	assert.False(t, hasNonStrictModeInvalidDate("2021--01-02"))
	assert.False(t, hasNonStrictModeInvalidDate("2021-01-02 03:04:05"))

	assert.True(t, hasNonStrictModeInvalidDate("2009-00-00"))
	assert.True(t, hasNonStrictModeInvalidDate("0000-00-00"))
	assert.True(t, hasNonStrictModeInvalidDate("0000-00-00 00:00:00"))
	assert.True(t, hasNonStrictModeInvalidDate("2009-00-00 00:00:00"))
}
