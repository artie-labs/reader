package iterator

type batchIterator[T any] struct {
	items []T
	index int
	n     int
}

func NewBatchIterator[T any](items []T, n int) *batchIterator[T] {
	return &batchIterator[T]{
		items: items,
		index: 0,
		n:     max(n, 1),
	}
}

func (i *batchIterator[T]) HasNext() bool {
	return i.index < len(i.items)
}

func (i *batchIterator[T]) Next() []T {
	if !i.HasNext() {
		return make([]T, 0)
	}
	end := min(i.index+i.n, len(i.items))
	result := i.items[i.index:end]
	i.index = end
	return result
}
