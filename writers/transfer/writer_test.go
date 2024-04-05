package transfer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToJSONTypes(t *testing.T) {
	{
		// Empty map.
		dataIn := map[string]any{}
		dataOut, err := toJSONTypes(dataIn)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{}, dataOut)
	}
	{
		// Non-empty map.
		dataIn := map[string]any{
			"nil":     nil,
			"int":     12345,
			"int64":   int64(123456),
			"float32": float32(12),
			"float64": float32(23),
			"binary":  []byte{byte(0), byte(1), byte(2), byte(3)},
		}
		dataOut, err := toJSONTypes(dataIn)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{
			"nil":     nil,
			"int":     float64(12345.0),
			"int64":   float64(123456.0),
			"float32": float64(12.0),
			"float64": float64(23.0),
			"binary":  "AAECAw==",
		}, dataOut)
	}
}
