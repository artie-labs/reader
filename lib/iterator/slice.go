package iterator

import "fmt"

type sliceIterator[T any] struct {
	index int
	items []T
}

// Returns an iterator that iterates over all the items in a slice.
func FromSlice[T any](items []T) Iterator[T] {
	return &sliceIterator[T]{items: items}
}

func (it *sliceIterator[T]) HasNext() bool {
	return it.index < len(it.items)
}

func (it *sliceIterator[T]) Next() (T, error) {
	if !it.HasNext() {
		var unused T
		return unused, fmt.Errorf("iterator has finished")
	}
	item := it.items[it.index]
	it.index++
	return item, nil
}

// Returns an iterator that produces a value once and then completes.
func Once[T any](value T) Iterator[T] {
	return FromSlice([]T{value})
}
