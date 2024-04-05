package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
)

type geomConverter struct {
	debeziumType debezium.SupportedDebeziumType
}

func (g geomConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Struct,
		DebeziumType: g.debeziumType,
	}
}

func (geomConverter) Convert(value any) (any, error) {
	mapValue, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any got %T with value: %v", value, value)
	}
	return mapValue, nil
}

func NewPointConverter() ValueConverter {
	return geomConverter{debeziumType: debezium.GeometryPointType}
}

func NewGeometryConverter() ValueConverter {
	return geomConverter{debeziumType: debezium.GeometryType}
}

func NewGeographyConverter() ValueConverter {
	return geomConverter{debeziumType: debezium.GeographyType}
}
