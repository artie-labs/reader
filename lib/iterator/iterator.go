package iterator

type batchIterator[T any] struct {
	items []T
	index int
	step  int
}

func NewBatchIterator[T any](items []T, n int) *batchIterator[T] {
	return &batchIterator[T]{
		items: items,
		index: 0,
		step:  max(n, 1),
	}
}

func (i *batchIterator[T]) HasNext() bool {
	return i.index < len(i.items)
}

func (i *batchIterator[T]) Next() []T {
	if !i.HasNext() {
		return nil
	}
	end := min(i.index+i.step, len(i.items))
	result := i.items[i.index:end]
	i.index = end
	return result
}
