package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToPoint(t *testing.T) {
	type _tc struct {
		name        string
		input       []byte
		output      *Point
		expectError bool
	}

	tcs := []_tc{
		{
			name:   "Valid point",
			input:  []byte("(2.2945,48.8584)"),
			output: &Point{X: 2.2945, Y: 48.8584},
		},
		{
			name:        "Invalid format",
			input:       []byte("2.2945,48.8584"),
			expectError: true,
		},
		{
			name:        "Invalid X coordinate",
			input:       []byte("(abc,48.8584)"),
			expectError: true,
		},
		{
			name:        "Invalid Y coordinate",
			input:       []byte("(2.2945,xyz)"),
			expectError: true,
		},
		{
			name:        "Empty input",
			input:       []byte(""),
			expectError: true,
		},
	}

	for _, tc := range tcs {
		point, err := ToPoint(tc.input)
		if tc.expectError {
			assert.Error(t, err, tc.name)
		} else {
			assert.Equal(t, *tc.output, *point, tc.name)
		}
	}
}

func TestToGeography(t *testing.T) {
	// TODO: We should make Transfer's `parseValue` function public so we can test for parsing symmetry
	{
		data := []byte("010100000000000000000000000000000000000000")
		expected, err := ToGeography(data)
		assert.NoError(t, err)

		// This is Point (0,0)
		assert.Equal(t, map[string]interface{}{
			"wkb":  "AQEAAAAAAAAAAAAAAAAAAAAAAAAA",
			"srid": nil,
		}, expected)
	}

	{
		data := []byte("0101000000000000000000F03F000000000000F03F")
		expected, err := ToGeography(data)
		assert.NoError(t, err)

		// This is Point (1,1)
		assert.Equal(t, map[string]interface{}{
			"wkb":  "AQEAAAAAAAAAAADwPwAAAAAAAPA/",
			"srid": nil,
		}, expected)
	}
}
