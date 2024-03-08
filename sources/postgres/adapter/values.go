package adapter

import (
	"github.com/artie-labs/reader/lib/postgres/schema"
)

func ConvertValueToDebezium(col schema.Column, value any) (any, error) {
	if value == nil {
		return value, nil
	}

	if converter := valueConverterForType(col.Type, col.Opts); converter != nil {
		return converter.Convert(value)
	}

	return value, nil
}
