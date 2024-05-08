package parse

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToPoint(t *testing.T) {
	tcs := []struct {
		name           string
		input          string
		output         *Point
		expectedOutput string
		expectedErr    string
	}{
		{
			name:           "Valid point",
			input:          "(2.2945,48.8584)",
			expectedOutput: `{"type":"Feature","geometry":{"type":"Point","coordinates":[2.2945,48.8584]}}`,
			output:         &Point{X: 2.2945, Y: 48.8584},
		},
		{
			name:        "Invalid format",
			input:       "2.2945,48.8584",
			expectedErr: "invalid point format",
		},
		{
			name:        "Invalid X coordinate",
			input:       "(abc,48.8584)",
			expectedErr: "invalid X coordinate",
		},
		{
			name:        "Invalid Y coordinate",
			input:       "(2.2945,xyz)",
			expectedErr: "invalid Y coordinate:",
		},
		{
			name:        "Empty input",
			input:       "",
			expectedErr: "invalid point format",
		},
	}

	for _, tc := range tcs {
		point, err := ToPoint(tc.input)
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		} else {
			assert.Equal(t, *tc.output, *point, tc.name)

			val, err := debezium.Field{DebeziumType: debezium.GeometryPointType}.ParseValue(point.ToMap())
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedOutput, val, tc.name)
		}
	}
}

func TestToGeography(t *testing.T) {
	{
		data := []byte("010100000000000000000000000000000000000000")
		geoData, err := ToGeography(data)
		assert.NoError(t, err)

		assert.Equal(t, map[string]any{
			"wkb":  "AQEAAAAAAAAAAAAAAAAAAAAAAAAA",
			"srid": nil,
		}, geoData)

		val, err := debezium.Field{DebeziumType: debezium.GeometryType}.ParseValue(geoData)
		assert.NoError(t, err)
		assert.Equal(t, `{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},"properties":null}`, val)
	}

	{
		data := []byte("0101000000000000000000F03F000000000000F03F")
		geoData, err := ToGeography(data)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{
			"wkb":  "AQEAAAAAAAAAAADwPwAAAAAAAPA/",
			"srid": nil,
		}, geoData)

		val, err := debezium.Field{DebeziumType: debezium.GeometryType}.ParseValue(geoData)
		assert.NoError(t, err)
		assert.Equal(t, `{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1]},"properties":null}`, val)
	}
}
