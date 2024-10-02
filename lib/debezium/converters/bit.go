package converters

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

func NewBitConverter(size int) BitConverter {
	return BitConverter{size: size}
}

type BitConverter struct {
	size int
}

func (BitConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Boolean,
	}
}

func (b BitConverter) Convert(value any) (any, error) {
	stringValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	switch b.size {
	case 0:
		return nil, fmt.Errorf("bit size is invalid")
	case 1:
		// This will be 0 (false) or 1 (true)
		if stringValue == "0" {
			return false, nil
		} else if stringValue == "1" {
			return true, nil
		}
		return nil, fmt.Errorf(`string value %q is not in ["0", "1"]`, value)
	default:
		return stringValue, nil
	}
}
