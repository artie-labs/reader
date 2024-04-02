package iterator

import "fmt"

type batchIterator[T any] struct {
	iter Iterator[T]
	step int
}

// Batch returns an iterator that splits a list of items into batches of the given step size.
func Batch[T any](iter Iterator[T], step int) Iterator[[]T] {
	return &batchIterator[T]{
		iter: iter,
		step: max(step, 1),
	}
}

func (bi *batchIterator[T]) HasNext() bool {
	return bi.iter.HasNext()
}

func (bi *batchIterator[T]) Next() ([]T, error) {
	if !bi.HasNext() {
		return nil, fmt.Errorf("batch iterator has finished")
	}

	var buffer []T
	for bi.HasNext() {
		item, err := bi.iter.Next()
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, item)
		if len(buffer) >= bi.step {
			break
		}
	}

	return buffer, nil
}
