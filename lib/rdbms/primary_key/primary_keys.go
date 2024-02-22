package primary_key

import (
	"fmt"
	"log/slog"
)

type Keys struct {
	keys   []Key
	keyMap map[string]bool
}

func NewKeys() *Keys {
	return &Keys{
		keyMap: make(map[string]bool),
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

func (k *Keys) Length() int {
	if k == nil {
		return 0
	}

	return len(k.keys)
}

func (k *Keys) Clone() *Keys {
	newKeys := NewKeys()
	for _, key := range k.keys {
		newKeys.keys = append(newKeys.keys, Key{key.Name, key.StartingValue, key.EndingValue})
	}
	for key, value := range k.keyMap {
		newKeys.keyMap[key] = value
	}
	return newKeys
}

func (k *Keys) Upsert(keyName string, startingVal *string, endingVal *string) {
	_, isOk := k.keyMap[keyName]
	if isOk {
		for index := range k.keys {
			if k.keys[index].Name == keyName {
				if startingVal != nil {
					k.keys[index].StartingValue = *startingVal
				}

				if endingVal != nil {
					k.keys[index].EndingValue = *endingVal
				}
				break
			}
		}
	} else {
		key := Key{
			Name: keyName,
		}

		if startingVal != nil {
			key.StartingValue = *startingVal
		}

		if endingVal != nil {
			key.EndingValue = *endingVal
		}

		k.keys = append(k.keys, key)
		k.keyMap[key.Name] = true
	}
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

// IsExausted returns true if the starting values and ending values are the same for all keys
func (k *Keys) IsExhausted() bool {
	for _, key := range k.keys {
		if key.StartingValue != key.EndingValue {
			return false
		}
	}
	return true
}
