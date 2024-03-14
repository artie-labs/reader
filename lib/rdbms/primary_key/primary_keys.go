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
			return fmt.Errorf("keys (%v), and passed in values (%v) length does not match, keys: %v, values: %s",
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
			return fmt.Errorf("keys (%v), and passed in values (%v) length does not match, keys: %v, values: %s",
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

func (k *Keys) UpdateStartingValue(keyName string, startingVal any) error {
	idx := slices.IndexFunc(k.keys, func(x Key) bool { return x.Name == keyName })
	if idx < 0 {
		return fmt.Errorf("no key named %s", keyName)
	}

	k.keys[idx].StartingValue = startingVal
	return nil
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

// IsExhausted returns true if the starting values and ending values are the same for all keys
func (k *Keys) IsExhausted() (bool, error) {
	for _, key := range k.keys {
		switch castStartValue := key.StartingValue.(type) {
		case []byte:
			castEndValue, ok := key.EndingValue.([]byte)
			if !ok {
				return false, fmt.Errorf(`start value is []byte but end value is %T for key "%s"`, key.EndingValue, key.Name)
			}
			return bytes.Equal(castStartValue, castEndValue), nil
		default:
			if key.StartingValue != key.EndingValue {
				return false, nil
			}
		}
	}
	return true, nil
}
