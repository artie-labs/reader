package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
)

type ArrayConverter struct{}

func (ArrayConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Array,
	}
}

func (ArrayConverter) Convert(value any) (any, error) {
	arrayValue, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected []any got %T with value: %v", value, value)
	}
	return arrayValue, nil
}
