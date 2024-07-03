package schema

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const DateTimeFormat = "2006-01-02 15:04:05.999999999"

// ConvertValue takes a value returned from the MySQL driver and converts it to a native Go type.
func ConvertValue(value any, colType DataType) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch colType {
	case Bit:
		// Bits
		castValue, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}
		if len(castValue) != 1 || castValue[0] > 1 {
			return nil, fmt.Errorf("bit value is invalid: %v", value)
		}
		return castValue[0] == 1, nil
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
		// Types we expect as int64
		_, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 got %T for value: %v", value, value)
		}
		return value, nil
	case Float:
		// Types we expect as float32
		_, ok := value.(float32)
		if !ok {
			return nil, fmt.Errorf("expected float32 got %T for value: %v", value, value)
		}
		return value, nil
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
		bytesValue, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}

		// MySQL supports 0000-00-00 for dates so we can't use time.Time
		return string(bytesValue), nil
	case DateTime, Timestamp:
		bytesValue, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}

		stringValue := string(bytesValue)
		if hasNonStrictModeInvalidDate(stringValue) {
			return nil, nil
		}

		timeValue, err := time.Parse(DateTimeFormat, stringValue)
		if err != nil {
			return nil, err
		}
		return timeValue, nil
	case Decimal,
		Time,
		Char,
		Varchar,
		Text,
		TinyText,
		MediumText,
		LongText,
		Enum,
		Set,
		JSON:
		// Types that we expect as a byte array that will be converted to strings
		switch castValue := value.(type) {
		case []byte:
			return string(castValue), nil
		case string:
			// The driver should return these these types as []byte but no reason not to support strings too
			return castValue, nil
		default:
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}
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
		convertedVal, err := ConvertValue(value, col.Type)
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
