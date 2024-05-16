package adapter

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/jackc/pgx/v5/pgtype"
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

type MSSQLTimeConverter struct{}

func (MSSQLTimeConverter) ToField(name string) debezium.Field {
	// Represents the number of milliseconds past midnight, and does not include timezone information.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int32,
		DebeziumType: debezium.Time,
	}
}

func (MSSQLTimeConverter) Convert(value any) (any, error) {
	// TODO:
	timeValue, ok := value.(pgtype.Time)
	if !ok {
		return nil, fmt.Errorf("expected pgtype.Time got %T with value: %v", value, value)
	}
	if !timeValue.Valid {
		return nil, nil
	}

	milliseconds := timeValue.Microseconds / int64(time.Millisecond/time.Microsecond)
	if milliseconds > math.MaxInt32 || milliseconds < math.MinInt32 {
		return nil, fmt.Errorf("milliseconds overflows int32")
	}
	return int32(milliseconds), nil
}

type MSSQLDatetime2Converter struct{}

func (MSSQLDatetime2Converter) ToField(name string) debezium.Field {

}
