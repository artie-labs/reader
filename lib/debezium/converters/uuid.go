package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
)

type UUIDConverter struct{}

func (UUIDConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.String,
		DebeziumType: debezium.UUID,
	}
}

func (UUIDConverter) Convert(value any) (any, error) {
	castValue, isOk := value.(string)
	if isOk {
		return castValue, nil
	}
	return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
}
