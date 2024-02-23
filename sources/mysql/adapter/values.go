package adapter

import (
	"github.com/artie-labs/reader/lib/mysql/schema"
)

func ConvertValueToDebezium(col schema.Column, value any) (any, error) {
	if value == nil {
		return value, nil
	}

	panic("not implemented")
}
