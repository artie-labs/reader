package converters

import "fmt"

type RowConverter struct {
	ValueConverters map[string]ValueConverter
}

func NewRowConverter(valueConverters map[string]ValueConverter) RowConverter {
	return RowConverter{ValueConverters: valueConverters}
}

func (r RowConverter) Convert(row map[string]any) (map[string]any, error) {
	result := make(map[string]any)
	for key, value := range row {
		valueConverter, isOk := r.ValueConverters[key]
		if !isOk {
			return nil, fmt.Errorf("failed to get ValueConverter for key %s", key)
		}

		if value != nil {
			var err error
			value, err = valueConverter.Convert(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value: %w", err)
			}
		}

		result[key] = value
	}
	return result, nil
}
