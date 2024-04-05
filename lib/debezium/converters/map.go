package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
)

type MapConverter struct{}

func (MapConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Map,
	}
}

func (MapConverter) Convert(value any) (any, error) {
	mapValue, ok := value.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any got %T with value: %v", value, value)
	}
	return mapValue, nil
}
