package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
)

type EnumConverter struct{}

func (EnumConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "string",
		DebeziumType: "io.debezium.data.Enum",
	}
}

func (EnumConverter) Convert(value any) (any, error) {
	castValue, isOk := value.(string)
	if isOk {
		return castValue, nil
	}

	return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
}

type EnumSetConverter struct{}

func (EnumSetConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "string",
		DebeziumType: "io.debezium.data.EnumSet",
	}
}

func (EnumSetConverter) Convert(value any) (any, error) {
	castValue, isOk := value.(string)
	if isOk {
		return castValue, nil
	}

	return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
}
