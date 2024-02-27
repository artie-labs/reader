package converters

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecimalConverter_Convert(t *testing.T) {
	converter := NewDecimalConverter(2, nil)
	{
		converted, err := converter.Convert("1.23")
		assert.NoError(t, err)

		field := debezium.Field{
			Parameters: map[string]any{
				"scale": converter.scale,
			},
		}

		actualValue, err := field.DecodeDecimal(fmt.Sprint(converted))
		assert.NoError(t, err)
		assert.Equal(t, "1.23", fmt.Sprint(actualValue))
	}
}
