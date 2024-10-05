package converters

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

// BitVaryingConverter - Precision here is optional, if it's not specified - then it's infinite.
// If it's specified, then it's the maximum number of bits that can be stored.
type BitVaryingConverter struct {
	optionalCharMaxLength int
}

func NewBitVaryingConverter(optionalCharMaxLength int) BitVaryingConverter {
	return BitVaryingConverter{optionalCharMaxLength: optionalCharMaxLength}
}

func (BitVaryingConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		DebeziumType: debezium.Bits,
		Type:         debezium.Bytes,
	}
}

func (b BitVaryingConverter) Convert(value any) (any, error) {
	stringValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	if b.optionalCharMaxLength > 0 && len(stringValue) > b.optionalCharMaxLength {
		return nil, fmt.Errorf("bit varying converter failed: value exceeds char max length, value: %q, length: %d", stringValue, len(stringValue))
	}

	for _, char := range stringValue {
		if char != '0' && char != '1' {
			return nil, fmt.Errorf("invalid binary string %q: contains non-binary characters", stringValue)
		}
	}

	return stringToByteA(stringValue)
}
