package adapter

import (
	"fmt"
	"math"
	"time"

	"github.com/artie-labs/reader/lib/timeutil"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/jackc/pgx/v5/pgtype"
)

type TimeWithTimezoneConverter struct{}

func (TimeWithTimezoneConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.String,
		DebeziumType: debezium.TimeWithTimezone,
	}
}

func (TimeWithTimezoneConverter) Convert(value any) (any, error) {
	stringValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	// No nanosecond precision because the data type only goes to ms.
	layouts := []string{
		"15:04:05-07",        // w/o fractional seconds
		"15:04:05.000-07",    // ms
		"15:04:05.000000-07", // microseconds
	}

	timeValue, err := timeutil.ParseExact(stringValue, layouts)
	if err != nil {
		return nil, fmt.Errorf("failed to parse time value %q: %w", stringValue, err)
	}

	// Convert `time.Time` into GMT
	// Then convert back into a string with ns precision
	return timeValue.UTC().Format(ext.PostgresTimeFormatNoTZ), nil
}

type PgTimeConverter struct{}

func (PgTimeConverter) ToField(name string) debezium.Field {
	// Represents the number of milliseconds past midnight, and does not include timezone information.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int32,
		DebeziumType: debezium.Time,
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

func (PgIntervalConverter) ToField(name string) debezium.Field {
	// The approximate number of microseconds for a time interval using the 365.25 / 12.0 formula for days per month average.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int64,
		DebeziumType: debezium.MicroDuration,
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
