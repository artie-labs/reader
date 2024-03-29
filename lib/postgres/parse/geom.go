package parse

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

type Point struct {
	X, Y float64
}

func (p *Point) ToMap() map[string]any {
	return map[string]any{
		"x": p.X,
		"y": p.Y,
	}
}

func ToPoint(data string) (*Point, error) {
	if !(strings.HasPrefix(data, "(") && strings.HasSuffix(data, ")")) {
		return nil, fmt.Errorf("invalid point format")
	}

	// Trim `(` and `)`
	trimmed := strings.Trim(data, "()")

	// Split the string by the comma
	parts := strings.Split(trimmed, ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid point data")
	}

	// Parse the X and Y coordinates
	x, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid X coordinate: %w", err)
	}

	y, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid Y coordinate: %w", err)
	}

	return &Point{X: x, Y: y}, nil
}

// ToGeography will take in a byte array (encoded in hex), decode it then b64 encode it and return it.
// Inspired by: https://github.com/twpayne/go-geom/issues/122#issuecomment-915170454
func ToGeography(data []byte) (map[string]any, error) {
	decodedBytes := make([]byte, hex.DecodedLen(len(data)))
	_, err := hex.Decode(decodedBytes, data)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"wkb":  base64.StdEncoding.EncodeToString(decodedBytes),
		"srid": nil,
	}, nil
}
