package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
)

type _geomConverter struct {
	debeziumType debezium.SupportedDebeziumType
}

func (g _geomConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "struct",
		DebeziumType: string(g.debeziumType),
	}
}

func (_geomConverter) Convert(value any) (any, error) {
	mapValue, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any got %T with value: %v", value, value)
	}

	return mapValue, nil
}

func NewPointConverter() ValueConverter {
	return _geomConverter{debeziumType: debezium.GeometryPointType}
}

func NewGeometryConverter() ValueConverter {
	return _geomConverter{debeziumType: debezium.GeometryType}
}

func NewGeographyConverter() ValueConverter {
	return _geomConverter{debeziumType: debezium.GeographyType}
}
