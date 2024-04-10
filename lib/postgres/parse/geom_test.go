package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToPoint(t *testing.T) {
	tcs := []struct {
		name        string
		input       string
		output      *Point
		expectedErr string
	}{
		{
			name:   "Valid point",
			input:  "(2.2945,48.8584)",
			output: &Point{X: 2.2945, Y: 48.8584},
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
		assert.Equal(t, map[string]any{
			"wkb":  "AQEAAAAAAAAAAAAAAAAAAAAAAAAA",
			"srid": nil,
		}, expected)
	}

	{
		data := []byte("0101000000000000000000F03F000000000000F03F")
		expected, err := ToGeography(data)
		assert.NoError(t, err)

		// This is Point (1,1)
		assert.Equal(t, map[string]any{
			"wkb":  "AQEAAAAAAAAAAADwPwAAAAAAAPA/",
			"srid": nil,
		}, expected)
	}
}
