package iterator

type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
}

type StreamingIterator[T any] interface {
	HasNext() bool
	Next() (T, error)
	CommitOffset()
}

// Collect returns a new slice containing all the items from an [Iterator].
// Used for testing, use only with iterators containing a finite amount of items that fit in memory.
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
