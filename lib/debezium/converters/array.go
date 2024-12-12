package converters

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"
)

func NewArrayConverter(elementType string) ArrayConverter {
	var json bool
	for _, el := range []string{"json", "jsonb"} {
		if strings.EqualFold(elementType, el) {
			json = true
			break
		}
	}

	return ArrayConverter{json: json}
}

type ArrayConverter struct {
	json bool
}

func (a ArrayConverter) ToField(name string) debezium.Field {
	var item *debezium.Item
	if a.json {
		item = &debezium.Item{
			DebeziumType: debezium.JSON,
		}
	}

	return debezium.Field{
		FieldName:     name,
		Type:          debezium.Array,
		ItemsMetadata: item,
	}
}

func (ArrayConverter) Convert(value any) (any, error) {
	arrayValue, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected []any got %T with value: %v", value, value)
	}
	return arrayValue, nil
}
