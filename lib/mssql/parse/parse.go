package parse

import (
	"fmt"
	"time"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/reader/lib/mssql/schema"
)

func ParseValue(colKind schema.DataType, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch colKind {
	case schema.Bit:
		if _, isOk := value.(bool); !isOk {
			return nil, fmt.Errorf("expected bool got %T with value: %v", value, value)
		}

		return value, nil
	case schema.Bytes:
		if _, isOk := value.([]byte); !isOk {
			return nil, fmt.Errorf("expected []byte got %T with value: %v", value, value)
		}

		return value, nil
	case schema.Int16, schema.Int32, schema.Int64:
		if _, isOk := value.(int64); !isOk {
			return nil, fmt.Errorf("expected int64 got %T with value: %v", value, value)
		}

		return value, nil
	case schema.Numeric, schema.Money:
		val, isOk := value.([]byte)
		if !isOk {
			return nil, fmt.Errorf("expected []byte got %T with value: %v", value, value)
		}

		return string(val), nil
	case schema.Float:
		if _, isOk := value.(float64); !isOk {
			return nil, fmt.Errorf("expected float64 got %T with value: %v", value, value)
		}

		return value, nil
	case schema.String:
		if _, isOk := value.(string); !isOk {
			return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
		}

		return value, nil
	case schema.UniqueIdentifier:
		var uniq mssql.UniqueIdentifier
		if err := uniq.Scan(value); err != nil {
			return nil, fmt.Errorf("failed to parse unique identifier value %q: %w", value, err)
		}

		return uniq.String(), nil
	case
		schema.Date,
		schema.Time, schema.TimeMicro, schema.TimeNano,
		schema.Datetime2, schema.Datetime2Micro, schema.Datetime2Nano,
		schema.DatetimeOffset:
		if _, isOk := value.(time.Time); !isOk {
			return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
		}

		return value, nil
	}

	return value, nil
}
