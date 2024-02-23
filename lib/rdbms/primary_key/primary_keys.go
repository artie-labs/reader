package primary_key

import (
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

func (k *Keys) LoadValues(startingValues, endingValues []string) error {
	length := len(k.keys)
	if len(startingValues) != 0 {
		if len(startingValues) != length {
			return fmt.Errorf("keys (%v), and passed in values (%v) length does not match, keys: %v, values: %s",
				length, len(startingValues), k.Keys(), startingValues)
		}

		for idx := range k.keys {
			newValue := startingValues[idx]

			slog.Info("Overriding primary key start value",
				slog.String("colName", k.keys[idx].Name),
				slog.Any("dbMin", k.keys[idx].StartingValue),
				slog.Any("override", newValue),
			)

			k.keys[idx].StartingValue = newValue
		}
	}

	if len(endingValues) != 0 {
		if len(endingValues) != length {
			return fmt.Errorf("keys (%v), and passed in values (%v) length does not match, keys: %v, values: %s",
				length, len(endingValues), k.Keys(), endingValues)
		}

		for idx := range k.keys {
			newValue := endingValues[idx]

			slog.Info("Overriding primary key end value",
				slog.String("colName", k.keys[idx].Name),
				slog.Any("dbMax", k.keys[idx].EndingValue),
				slog.Any("override", newValue),
			)

			k.keys[idx].EndingValue = endingValues[idx]
		}
	}

	return nil
}

func (k *Keys) Clone() *Keys {
	return NewKeys(k.keys)
}

func (k *Keys) UpdateStartingValue(keyName string, startingVal any) error {
	idx := slices.IndexFunc(k.keys, func(x Key) bool { return x.Name == keyName })
	if idx < 0 {
		return fmt.Errorf("could not find key named %s", keyName)
	}

	k.keys[idx].StartingValue = startingVal
	return nil
}

func (k *Keys) Keys() []string {
	var keysToReturn []string
	for _, key := range k.keys {
		keysToReturn = append(keysToReturn, key.Name)
	}
	return keysToReturn
}

func (k *Keys) KeysList() []Key {
	return k.keys
}

// IsExhausted returns true if the starting values and ending values are the same for all keys
func (k *Keys) IsExhausted() bool {
	for _, key := range k.keys {
		if key.StartingValue != key.EndingValue {
			return false
		}
	}
	return true
}
