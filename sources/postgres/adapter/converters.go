package adapter

import (
	"fmt"
	"math"
	"time"

	transferDbz "github.com/artie-labs/transfer/lib/debezium"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/stringutil"
)

const moneyScale = 2

type MoneyConverter struct{}

func (MoneyConverter) ToField(name string) transferDbz.Field {
	return transferDbz.Field{
		FieldName:    name,
		Type:         "bytes",
		DebeziumType: string(transferDbz.KafkaDecimalType),
		Parameters: map[string]any{
			"scale": fmt.Sprint(moneyScale),
		},
	}
}

func (MoneyConverter) Convert(value any) (any, error) {
	stringValue := stringutil.ParseMoneyIntoString(fmt.Sprint(value))
	return debezium.EncodeDecimalToBytes(stringValue, moneyScale), nil
}

type PgTimeConverter struct{}

func (PgTimeConverter) ToField(name string) transferDbz.Field {
	// Represents the number of milliseconds past midnight, and does not include timezone information.
	return transferDbz.Field{
		FieldName:    name,
		Type:         "int32",
		DebeziumType: string(transferDbz.Time),
	}
}

func (PgTimeConverter) Convert(value any) (any, error) {
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

type PgIntervalConverter struct{}

func (PgIntervalConverter) ToField(name string) transferDbz.Field {
	// The approximate number of microseconds for a time interval using the 365.25 / 12.0 formula for days per month average.
	return transferDbz.Field{
		FieldName:    name,
		Type:         "int64",
		DebeziumType: "io.debezium.time.MicroDuration",
	}
}

func (PgIntervalConverter) Convert(value any) (any, error) {
	intervalValue, ok := value.(pgtype.Interval)
	if !ok {
		return nil, fmt.Errorf("expected pgtype.Interval got %T with value: %v", value, value)
	}
	if !intervalValue.Valid {
		return nil, nil
	}

	totalDays := float64(intervalValue.Days) + float64(intervalValue.Months)*365.25/12.0
	microsecondsInDay := float64((time.Duration(24) * time.Hour) / time.Microsecond)
	if totalDays > math.MaxInt64/microsecondsInDay {
		return nil, fmt.Errorf("positive microseconds are too large for an int64")
	} else if totalDays < math.MinInt64/microsecondsInDay {
		return nil, fmt.Errorf("negative microseconds are too large for an int64")
	}
	daysInMicroseconds := int64(totalDays * microsecondsInDay)

	if daysInMicroseconds > 0 && intervalValue.Microseconds > 0 {
		if daysInMicroseconds > math.MaxInt64-intervalValue.Microseconds {
			return nil, fmt.Errorf("positive microseconds are too large for an int64")
		}
	} else if daysInMicroseconds < 0 && intervalValue.Microseconds < 0 {
		if daysInMicroseconds < math.MinInt64+intervalValue.Microseconds {
			return nil, fmt.Errorf("negative microseconds are too large for an int64")
		}
	}

	return intervalValue.Microseconds + daysInMicroseconds, nil
}
