package adapter

import (
	"github.com/artie-labs/reader/lib/mysql/schema"
)

func ConvertValueToDebezium(col schema.Column, value interface{}) (interface{}, error) {
	if value == nil {
		return value, nil
	}

	panic("not implemented")
}
