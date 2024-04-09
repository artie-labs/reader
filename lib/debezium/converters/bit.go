package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
)

type BitConverter struct{}

func (BitConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Boolean,
	}
}

func (BitConverter) Convert(value any) (any, error) {
	// This will be 0 (false) or 1 (true)
	stringValue, ok := value.(string)
	if ok {
		if stringValue == "0" {
			return false, nil
		} else if stringValue == "1" {
			return true, nil
		}
		return nil, fmt.Errorf(`string value %q is not in ["0", "1"]`, value)
	}
	return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
}
