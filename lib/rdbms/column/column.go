package column

import (
	"fmt"
	"slices"
)

type Column[T ~int, O any] struct {
	Name string
	Type T
	Opts *O
}

func GetColumnByName[T ~int, O any](columns []Column[T, O], name string) (*Column[T, O], error) {
	index := slices.IndexFunc(columns, func(c Column[T, O]) bool { return c.Name == name })
	if index < 0 {
		return nil, fmt.Errorf("failed to find column with name: %s", name)
	}
	return &columns[index], nil
}

func GetColumnsByName[T ~int, O any](columns []Column[T, O], names []string) ([]Column[T, O], error) {
	var result []Column[T, O]
	for _, name := range names {
		col, err := GetColumnByName(columns, name)
		if err != nil {
			return nil, err
		}
		result = append(result, *col)
	}
	return result, nil
}

// FilterOutExcludedColumns returns a list of columns excluding those that match `excludeNames` provided they are not primary keys.
func FilterOutExcludedColumns[T ~int, O any](columns []Column[T, O], excludeNames []string, primaryKeys []string) ([]Column[T, O], error) {
	if len(excludeNames) == 0 {
		return columns, nil
	}

	var result []Column[T, O]
	for _, column := range columns {
		if slices.Contains(excludeNames, column.Name) {
			if slices.Contains(primaryKeys, column.Name) {
				return nil, fmt.Errorf(`cannot exclude primary key column "%s""`, column.Name)
			}
		} else {
			result = append(result, column)
		}
	}
	return result, nil
}
