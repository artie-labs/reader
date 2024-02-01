package iterator

type ArrayIterator[T any] interface {
	HasNext() bool
	Next() []T
}

func Collect[T any](iter ArrayIterator[T]) []T {
	result := make([]T, 0)
	for iter.HasNext() {
		result = append(result, iter.Next()...)
	}
	return result
}

type batchIterator[T any] struct {
	items []T
	index int
	n     int
}

func NewBatchIterator[T any](items []T, n int) *batchIterator[T] {
	return &batchIterator[T]{
		items: items,
		index: 0,
		n:     n,
	}
}

func (i *batchIterator[T]) HasNext() bool {
	return i.index < len(i.items)
}

func (i *batchIterator[T]) Next() []T {
	if !i.HasNext() {
		return make([]T, 0)
	}
	result := i.items[i.index:min(i.index+i.n, len(i.items))]
	i.index += i.n
	return result
}
