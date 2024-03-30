package iterator

import "fmt"

type prebatchedIterator[T any] struct {
	index   int
	batches [][]T
}

// Returns an iterator that produces multiple predefined batches - used with tests.
func MultiBatchIterator[T any](batches [][]T) Iterator[[]T] {
	return &prebatchedIterator[T]{batches: batches}
}

func (pi *prebatchedIterator[T]) HasNext() bool {
	return pi.index < len(pi.batches)
}

func (pi *prebatchedIterator[T]) Next() ([]T, error) {
	if !pi.HasNext() {
		return nil, fmt.Errorf("iterator has finished")
	}
	result := pi.batches[pi.index]
	pi.index++
	return result, nil
}

// Returns an iterator that produces a single predefined batch - used as a shim for DynamoDB.
func SingleBatchIterator[T any](batches []T) Iterator[[]T] {
	return MultiBatchIterator([][]T{batches})
}
