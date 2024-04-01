package iterator

type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
}

// Collect returns a new slice containing all the items from an [Iterator].
func Collect[T any](iter Iterator[T]) ([]T, error) {
	var result []T
	for iter.HasNext() {
		value, err := iter.Next()
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}
