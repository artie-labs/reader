package primary_key

import (
	"bytes"
	"fmt"
	"log/slog"
	"slices"
)

type Keys struct {
	keys []Key
}

func NewKeys(keys []Key) *Keys {
	return &Keys{
		keys: slices.Clone(keys),
	}
}

func (k *Keys) LoadValues(startingValues, endingValues []any) error {
	length := len(k.keys)
	if len(startingValues) != 0 {
		if len(startingValues) != length {
			return fmt.Errorf("keys (%d), and passed in values (%d) length does not match, keys: %v, values: %v",
				length, len(startingValues), k.KeyNames(), startingValues)
		}

		for idx, key := range k.keys {
			newValue := startingValues[idx]

			slog.Info("Overriding primary key start value",
				slog.String("colName", key.Name),
				slog.Any("dbMin", key.StartingValue),
				slog.Any("override", newValue),
			)

			k.keys[idx].StartingValue = newValue
		}
	}

	if len(endingValues) != 0 {
		if len(endingValues) != length {
			return fmt.Errorf("keys (%d), and passed in values (%d) length does not match, keys: %v, values: %v",
				length, len(endingValues), k.KeyNames(), endingValues)
		}

		for idx, key := range k.keys {
			newValue := endingValues[idx]

			slog.Info("Overriding primary key end value",
				slog.String("colName", key.Name),
				slog.Any("dbMax", key.EndingValue),
				slog.Any("override", newValue),
			)

			k.keys[idx].EndingValue = endingValues[idx]
		}
	}

	return nil
}

// UpdateStartingValue sets the starting value for a primary key and returns whether the value changed.
func (k *Keys) UpdateStartingValue(keyName string, startingVal any) (bool, error) {
	idx := slices.IndexFunc(k.keys, func(x Key) bool { return x.Name == keyName })
	if idx < 0 {
		return false, fmt.Errorf("no key named %q", keyName)
	}

	changed := !equal(k.keys[idx].StartingValue, startingVal)
	k.keys[idx].StartingValue = startingVal
	return changed, nil
}

func (k *Keys) KeyNames() []string {
	var keysToReturn []string
	for _, key := range k.keys {
		keysToReturn = append(keysToReturn, key.Name)
	}
	return keysToReturn
}

func (k *Keys) Keys() []Key {
	return k.keys
}

// IsExhausted returns true if the starting values and ending values are the same for all keys.
func (k *Keys) IsExhausted() bool {
	for _, key := range k.keys {
		if !equal(key.StartingValue, key.EndingValue) {
			return false
		}
	}
	return true
}

func equal(a, b any) bool {
	// Comparing byte arrays panics: comparing uncomparable type []uint8.
	if aBytes, ok := a.([]byte); ok {
		bBytes, ok := b.([]byte)
		if !ok {
			return false
		}
		return bytes.Equal(aBytes, bBytes)
	} else if _, ok := b.([]byte); ok {
		return false // b is []byte but a is not
	}

	return a == b
}
