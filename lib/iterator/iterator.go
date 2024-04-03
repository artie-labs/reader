package iterator

type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
}

// Collect returns a new slice containing all the items from an [Iterator].
// Used for testing, use only with iterators containing a finite amount of items that fit in memory.
func Collect[T any](iter FunctionalIterator[T]) ([]T, error) {
	var result []T
	for {
		item, err, ok := iter()
		if !ok {
			break
		} else if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

// TODO: Replace all uses of [Iterator] with [FunctionalIterator] and then rename to [Iterator].
type FunctionalIterator[T any] func() (value T, err error, ok bool)

func ToFunctionalIterator[T any](iter Iterator[T]) FunctionalIterator[T] {
	return func() (T, error, bool) {
		if iter.HasNext() {
			item, err := iter.Next()
			return item, err, true
		}
		var empty T
		return empty, nil, false
	}
}
