package converters

import (
	"encoding/json"
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

func (a ArrayConverter) Convert(value any) (any, error) {
	arrayValue, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected []any got %T with value: %v", value, value)
	}

	if a.json {
		// If json is enabled, we should parse the array elements to JSON strings
		var elements []any
		for _, el := range arrayValue {
			switch el.(type) {
			case string:
				// Already JSON string, so we can skip the marshalling
				elements = append(elements, el)
			default:
				parsedValue, err := json.Marshal(el)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal value: %v", err)
				}

				elements = append(elements, string(parsedValue))
			}
		}

		return elements, nil
	}

	return arrayValue, nil
}
