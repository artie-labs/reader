package converters

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium"
	"strings"
)

const moneyScale = 2

type MoneyConverter struct {
	// MutateString will remove commas and currency symbols
	MutateString bool
}

func (MoneyConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Bytes,
		DebeziumType: debezium.KafkaDecimalType,
		Parameters: map[string]any{
			"scale": fmt.Sprint(moneyScale),
		},
	}
}

// Convert will change $4,000 to 4000.
func (m MoneyConverter) Convert(value any) (any, error) {
	valString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	if m.MutateString {
		valString = strings.Replace(valString, "$", "", 1)
		valString = strings.ReplaceAll(valString, ",", "")
	}

	return debezium.EncodeDecimal(valString, moneyScale)
}
