package parse

import (
	"fmt"
	mssql "github.com/microsoft/go-mssqldb"
	"time"

	"github.com/artie-labs/reader/lib/mssql/schema"
)

func ParseValue(colKind schema.DataType, value any) (any, error) {
	// If the value is nil - just return.
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
	case schema.Numeric:
		val, isOk := value.([]uint8)
		if !isOk {
			return nil, fmt.Errorf("expected []uint8 got %T with value: %v", value, value)
		}

		return string(val), nil
	case schema.Float:
		if _, isOk := value.(float64); !isOk {
			return nil, fmt.Errorf("expected float64 got %T with value: %v", value, value)
		}

		return value, nil
	case schema.Money:
		val, isOk := value.([]uint8)
		if !isOk {
			return nil, fmt.Errorf("expected []uint8 got %T with value: %v", value, value)
		}

		return string(val), nil
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
	case schema.Date, schema.Time, schema.TimeMicro, schema.TimeNano:
		if _, isOk := value.(time.Time); !isOk {
			return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
		}

		return value, nil
	}

	fmt.Println(fmt.Sprintf("colKind: %v, value: %v, type: %T", colKind, value, value))
	return value, nil
}
