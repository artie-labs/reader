package schema

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

func asSet(val any, opts []string) (string, error) {
	if castedValue, ok := val.(int64); ok {
		var out []string
		for i, opt := range opts {
			if castedValue&(1<<uint(i)) > 0 {
				out = append(out, opt)
			}
		}

		return strings.Join(out, ","), nil
	}

	// Snapshot will emit it as a string
	return asString(val)
}

func asEnum(val any, opts []string) (string, error) {
	if castedValue, ok := val.(int64); ok {
		if int(castedValue) >= len(opts) {
			return "", fmt.Errorf("enum value %d not in range [0, %d]", castedValue, len(opts)-1)
		}
		return opts[castedValue], nil
	}

	// Snapshot will emit it as a string
	return asString(val)
}

func asInt64(val any) (int64, error) {
	switch castedValue := val.(type) {
	case int64:
		return castedValue, nil
	case int32:
		return int64(castedValue), nil
	case int16:
		return int64(castedValue), nil
	case int8:
		return int64(castedValue), nil
	case int:
		return int64(castedValue), nil
	default:
		return 0, fmt.Errorf("expected integers, got %T with value: %v", val, val)
	}
}

func asString(val any) (string, error) {
	switch castedValue := val.(type) {
	case string:
		return castedValue, nil
	case []byte:
		return string(castedValue), nil
	default:
		return "", fmt.Errorf("expected string or []byte got %T for value: %v", val, val)
	}
}

func asFloat32(val any) (float32, error) {
	switch castedValue := val.(type) {
	case float32:
		return castedValue, nil
	case float64:
		if castedValue > math.MaxFloat32 || castedValue < -math.MaxFloat32 {
			return 0, fmt.Errorf("value overflows float32")
		}

		return float32(castedValue), nil
	default:
		return 0, fmt.Errorf("expected float32 or float64 got %T with value: %v", val, val)
	}
}

const DateTimeFormat = "2006-01-02 15:04:05.999999999"

// ConvertValue takes a value returned from the MySQL driver and converts it to a native Go type.
func ConvertValue(value any, colType DataType, opts *Opts) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch colType {
	case Bit:
		if opts == nil || opts.Size == nil {
			return nil, fmt.Errorf("bit column has no size")
		}

		// Bits
		castValue, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}

		switch *opts.Size {
		case 0:
			return nil, fmt.Errorf("bit column has size 0, valid range is [1, 64]")
		case 1:
			if len(castValue) != 1 || castValue[0] > 1 {
				return nil, fmt.Errorf("bit value is invalid: %v", value)
			}
			return castValue[0] == 1, nil
		default:
			return castValue, nil
		}
	case Boolean:
		castVal, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 got %T for value: %v", value, value)
		}
		if castVal > 1 || castVal < 0 {
			return nil, fmt.Errorf("boolean value %d not in [0, 1]", castVal)
		}
		return castVal == 1, nil
	case TinyInt,
		SmallInt,
		MediumInt,
		Int,
		BigInt,
		Year:
		return asInt64(value)
	case Float:
		return asFloat32(value)
	case Double:
		// Types we expect as float64
		_, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64 got %T for value: %v", value, value)
		}
		return value, nil
	case Binary, Varbinary, Blob:
		// Types we expect as a byte array
		_, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}
		return value, nil
	case Date:
		// MySQL supports 0000-00-00 for dates so we can't use time.Time
		return asString(value)
	case DateTime, Timestamp:
		stringValue, err := asString(value)
		if err != nil {
			return nil, err
		}

		if hasNonStrictModeInvalidDate(stringValue) {
			return nil, nil
		}

		timeValue, err := time.Parse(DateTimeFormat, stringValue)
		if err != nil {
			return nil, err
		}
		return timeValue, nil
	case Enum:
		if opts == nil {
			return nil, fmt.Errorf("enum column has no options")
		}

		return asEnum(value, opts.EnumValues)
	case Set:
		if opts == nil {
			return nil, fmt.Errorf("set column has no options")
		}

		return asSet(value, opts.EnumValues)
	case Decimal,
		Time,
		Char,
		Varchar,
		Text,
		TinyText,
		MediumText,
		LongText,
		JSON:
		// Types that we expect as a byte array that will be converted to strings
		return asString(value)
	case Point:
		bytes, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}

		// Byte format is https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html#:~:text=the%20OpenGIS%20specification.-,Internal%20Geometry%20Storage%20Format,-MySQL%20stores%20geometry
		if len(bytes) != 25 {
			return nil, fmt.Errorf("expected []byte with length 25, length is %d", len(bytes))
		}

		// The first four bytes are the SRID.
		if byteOrder := bytes[4]; byteOrder != 1 {
			return nil, fmt.Errorf("expected byte order to be 1 (little-endian), byte order is %d", byteOrder)
		}

		if integerType := binary.LittleEndian.Uint32(bytes[5:9]); integerType != 1 {
			return nil, fmt.Errorf("expected integer type 1 (POINT), got %d", integerType)
		}

		return map[string]any{
			"x": math.Float64frombits(binary.LittleEndian.Uint64(bytes[9:17])),
			"y": math.Float64frombits(binary.LittleEndian.Uint64(bytes[17:25])),
		}, nil
	case Geometry:
		bytes, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}

		if len(bytes) < 25 {
			return nil, fmt.Errorf("expected []byte with at least length 25, length is %d", len(bytes))
		}

		var byteOrder binary.ByteOrder
		switch bytes[4] {
		case 0:
			byteOrder = binary.BigEndian
		case 1:
			byteOrder = binary.LittleEndian
		default:
			return nil, fmt.Errorf("invalid byte order %d", bytes[4])
		}

		return map[string]any{
			"wkb": bytes[4:],
			// The first 4 bytes indicate the SRID
			"srid": byteOrder.Uint32(bytes[0:4]),
		}, nil
	}

	return nil, fmt.Errorf("could not convert DataType(%d) %T value: %v", colType, value, value)
}

// ConvertValues takes values returned from the MySQL driver and converts them to native Go types.
func ConvertValues(values []any, cols []Column) error {
	if len(values) != len(cols) {
		return fmt.Errorf("values and cols are not the same length")
	}

	for i, value := range values {
		col := cols[i]
		convertedVal, err := ConvertValue(value, col.Type, col.Opts)
		if err != nil {
			return fmt.Errorf("failed to convert value for column %q: %w", col.Name, err)
		}
		values[i] = convertedVal
	}
	return nil
}

// hasNonStrictModeInvalidDate - if strict mode is not enabled, we can end up having invalid datetimes
func hasNonStrictModeInvalidDate(d string) bool {
	if len(d) < 10 {
		return false
	}

	parts := strings.Split(d[:10], "-")
	if len(parts) != 3 {
		return false
	}

	// Year, month, date cannot be non-zero
	for _, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil {
			return false
		}
		if value == 0 {
			return true
		}
	}
	return false
}

func peek(s string, position uint) (byte, bool) {
	if len(s) <= int(position) {
		return 0, false
	}

	return s[position], true
}

// parseEnumValues will parse the metadata string for an ENUM or SET column and return the values.
// Note: This was not implemented using Go's CSV stdlib as we cannot modify the quote char from `"` to `'`. Ref: https://github.com/golang/go/issues/8458
func parseEnumValues(metadata string) ([]string, error) {
	var quoteByte byte = '\''
	var result []string
	var current strings.Builder
	var inQuotes bool

	for i := 0; i < len(metadata); i++ {
		char := metadata[i]
		switch char {
		case quoteByte:
			if inQuotes {
				if nextChar, ok := peek(metadata, uint(i+1)); ok && nextChar == quoteByte {
					current.WriteByte(quoteByte)
					i++
				} else {
					inQuotes = false
				}
			} else {
				inQuotes = true
			}
		case ',':
			if inQuotes {
				current.WriteByte(char)
			} else {
				result = append(result, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(char)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result, nil
}
