package iterator

import "fmt"

type batchIterator[T any] struct {
	items []T
	index int
	step  int
}

// Returns an iterater that splits a list of items into batches of the given step size.
func BatchIterator[T any](items []T, step int) Iterator[[]T] {
	return &batchIterator[T]{
		items: items,
		index: 0,
		step:  max(step, 1),
	}
}

func (i *batchIterator[T]) HasNext() bool {
	return i.index < len(i.items)
}

func (i *batchIterator[T]) Next() ([]T, error) {
	if !i.HasNext() {
		return nil, fmt.Errorf("iterator has finished")
	}
	end := min(i.index+i.step, len(i.items))
	result := i.items[i.index:end]
	i.index = end
	return result, nil
}
