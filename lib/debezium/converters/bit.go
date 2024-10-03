package converters

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

func NewBitConverter(charMaxLength int) BitConverter {
	return BitConverter{
		charMaxLength: charMaxLength,
	}
}

type BitConverter struct {
	charMaxLength int
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

	switch b.charMaxLength {
	case 0:
		return nil, fmt.Errorf("bit converter failed: invalid char max length")
	case 1:
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
