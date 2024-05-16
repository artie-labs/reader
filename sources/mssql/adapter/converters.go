package adapter

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium"
)

const moneyScale = 2

type MoneyConverter struct{}

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
func (MoneyConverter) Convert(value any) (any, error) {
	valueString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	return debezium.EncodeDecimal(valueString, moneyScale)
}
