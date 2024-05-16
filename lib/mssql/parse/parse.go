package parse

import (
	"fmt"

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
	}

	fmt.Println(fmt.Sprintf("colKind: %v, value: %v, type: %T", colKind, value, value))
	return value, nil
}
