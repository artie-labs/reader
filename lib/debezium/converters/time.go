package converters

import (
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/artie-labs/transfer/lib/debezium"
)

// Represents the number of milliseconds past midnight, and does not include timezone information.
type TimeConverter struct{ inputUnit time.Duration }

func NewTimeConverter(inputUnit time.Duration) TimeConverter {
	return TimeConverter{inputUnit: inputUnit}
}

func (TimeConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int32",
		DebeziumType: string(debezium.Time),
	}
}

func (t TimeConverter) Convert(value any) (any, error) {
	int64Value, err := asInt64(value)
	if err != nil {
		return nil, err
	}
	result := (time.Duration(int64Value) * t.inputUnit) / time.Millisecond
	if result > math.MaxInt32 {
		return nil, fmt.Errorf("millisecond value is larger than MaxInt32: %d", result)
	} else if result < math.MinInt32 {
		return nil, fmt.Errorf("millisecond value is smaller than MinInt32: %d", result)
	}
	return int32(result), nil
}

// Represents the number of microseconds past midnight, and does not include timezone information.
type MicroTimeConverter struct{}

func (MicroTimeConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int64",
		DebeziumType: string(debezium.TimeMicro),
	}
}

func (MicroTimeConverter) Convert(value any) (any, error) {
	strValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}
	timeValue, err := time.Parse(time.TimeOnly, strValue)
	if err != nil {
		return nil, err
	}

	hours := time.Duration(timeValue.Hour()) * time.Hour
	minutes := time.Duration(timeValue.Minute()) * time.Minute
	seconds := time.Duration(timeValue.Second()) * time.Second
	return int64((hours + minutes + seconds) / time.Microsecond), nil
}

// The approximate number of microseconds for a time interval using the 365.25 / 12.0 formula for days per month average.
type MicroDurationConverter struct{ inputUnit time.Duration }

func NewMicroDurationConverter(inputUnit time.Duration) MicroDurationConverter {
	return MicroDurationConverter{inputUnit: inputUnit}
}

func (MicroDurationConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int64",
		DebeziumType: "io.debezium.time.MicroDuration",
	}
}

func (m MicroDurationConverter) Convert(value any) (any, error) {
	int64Value, err := asInt64(value)
	if err != nil {
		return nil, err
	}

	if m.inputUnit > time.Microsecond {
		// Minimize overflows by computing the unit conversion first
		unitConversion := int64(m.inputUnit / time.Microsecond)
		if int64Value > math.MaxInt64/unitConversion {
			return nil, fmt.Errorf("microsecond value is larger than MaxInt64")
		} else if int64Value < math.MinInt64/unitConversion {
			return nil, fmt.Errorf("microsecond value is smaller than MinInt64")
		}
		return int64Value * unitConversion, nil
	} else {
		return int64Value / (int64(time.Microsecond / m.inputUnit)), nil
	}
}

type DateConverter struct{}

func (DateConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int32",
		DebeziumType: string(debezium.Date),
	}
}

func (DateConverter) Convert(value any) (any, error) {
	var timeValue time.Time
	switch castValue := value.(type) {
	case time.Time:
		timeValue = castValue
	case string:
		if castValue == "0000-00-00" {
			// MySQL supports '0000-00-00' for date columns
			return nil, nil
		}
		var err error
		timeValue, err = time.Parse(time.DateOnly, castValue)
		if err != nil {
			return nil, fmt.Errorf("failed to convert to date: %w", err)
		}
	default:
		return nil, fmt.Errorf("expected string/time.Time got %T with value: %v", value, value)
	}

	return int32(timeValue.Unix() / (60 * 60 * 24)), nil
}

type TimestampConverter struct{}

func (TimestampConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "string",
		DebeziumType: string(debezium.Timestamp),
	}
}

func (TimestampConverter) Convert(value any) (any, error) {
	timeValue, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
	}

	if timeValue.Year() > 9999 || timeValue.Year() < 0 {
		// Avoid copying this column over because it'll cause a JSON Marshal error:
		// Time.MarshalJSON: year outside of range [0,9999]
		slog.Info("Skipping timestamp because year is greater than 9999 or less than 0", slog.Any("value", value))
		return nil, nil
	}

	return timeValue, nil
}

type YearConverter struct{}

func (YearConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int32",
		DebeziumType: "io.debezium.time.Year",
	}
}

func (YearConverter) Convert(value any) (any, error) {
	return asInt32(value)
}
