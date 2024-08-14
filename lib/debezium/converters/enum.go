package converters

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

type EnumConverter struct{}

func (EnumConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.String,
		DebeziumType: debezium.Enum,
	}
}

func (EnumConverter) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	return castedValue, nil
}

type EnumSetConverter struct{}

func (EnumSetConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.String,
		DebeziumType: debezium.EnumSet,
	}
}

func (EnumSetConverter) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	return castedValue, nil
}
