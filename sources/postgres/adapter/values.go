package adapter

import (
	"log/slog"
	"time"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func ConvertValueToDebezium(col schema.Column, value any) (any, error) {
	if value == nil {
		return value, nil
	}

	if converter := valueConverterForType(col.Type, col.Opts); converter != nil {
		return converter.Convert(value)
	}

	switch col.Type {
	case schema.Timestamp:
		valTime, isOk := value.(time.Time)
		if isOk {
			if valTime.Year() > 9999 || valTime.Year() < 0 {
				// Avoid copying this column over because it'll cause a JSON Marshal error:
				// Time.MarshalJSON: year outside of range [0,9999]
				slog.Info("Skipping timestamp because year is greater than 9999 or less than 0", slog.String("key", col.Name), slog.Any("value", value))
				return nil, nil
			}
		}
	}

	return value, nil
}
