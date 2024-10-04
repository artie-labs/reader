package converters

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

func NewBitConverter(charMaxLength int) BitConverter {
	return BitConverter{charMaxLength: charMaxLength}
}

type BitConverter struct {
	charMaxLength int
}

func (b BitConverter) ToField(name string) debezium.Field {
	switch b.charMaxLength {
	case 1:
		return debezium.Field{FieldName: name, Type: debezium.Boolean}
	default:
		return debezium.Field{
			FieldName:    name,
			DebeziumType: debezium.Bits,
			Type:         debezium.Bytes,
			Parameters:   map[string]any{"length": fmt.Sprint(b.charMaxLength)},
		}
	}
}

func (b BitConverter) Convert(value any) (any, error) {
	stringValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	if b.charMaxLength == 0 {
		return nil, fmt.Errorf("bit converter failed: invalid char max length")
	}

	if len(stringValue) != b.charMaxLength {
		return nil, fmt.Errorf("bit converter failed: mismatched char max length, value: %q, length: %d", stringValue, len(stringValue))
	}

	switch b.charMaxLength {
	case 1:
		// For bit, bit(1) - We will convert these to booleans
		if stringValue == "0" {
			return false, nil
		} else if stringValue == "1" {
			return true, nil
		}
		return nil, fmt.Errorf(`string value %q is not in ["0", "1"]`, value)
	default:
		for _, char := range stringValue {
			if char != '0' && char != '1' {
				return nil, fmt.Errorf("invalid binary string %q: contains non-binary characters", stringValue)
			}
		}

		intValue, err := strconv.ParseInt(stringValue, 2, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert binary string %q to integer: %w", stringValue, err)
		}

		return intToByteA(
			intValue,
			// Calculate the number of bytes by dividing the number of bits by 8 and rounding up
			int(math.Ceil(float64(b.charMaxLength)/8.0)),
		)
	}
}

// intToByteA - Converts an integer to a byte array of the specified length, using little endian, which mirrors the same logic as java.util.BitSet
// Ref: https://docs.oracle.com/javase/7/docs/api/java/util/BitSet.html
func intToByteA(intValue int64, byteLength int) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, intValue); err != nil {
		return nil, fmt.Errorf("failed to write bytes: %w", err)
	}

	// Truncate the buffer to the required length (because binary.Write will write 8 bytes for an int64)
	result := buf.Bytes()
	return result[:byteLength], nil
}
