package adapter

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium"
	"strings"
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
	// TODO: Not going to work
	stringValue := strings.Replace(fmt.Sprint(value), "$", "", 1)
	stringValue = strings.ReplaceAll(stringValue, ",", "")
	return debezium.EncodeDecimal(stringValue, moneyScale)
}
