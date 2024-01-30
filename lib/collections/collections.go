package collections

type ArrayIterator[T any] interface {
	HasNext() bool
	Next() ([]T, error)
}

func Collect[T any](iter ArrayIterator[T]) ([]T, error) {
	result := make([]T, 0)
	for iter.HasNext() {
		items, err := iter.Next()
		if err != nil {
			return nil, err
		}
		result = append(result, items...)
	}
	return result, nil
}

type chunkIterator[T any] struct {
	items []T
	index int
	size  int
}

func NewChunkIterator[T any](items []T, size int) *chunkIterator[T] {
	return &chunkIterator[T]{
		items: items,
		index: 0,
		size:  size,
	}
}

func (i *chunkIterator[T]) HasNext() bool {
	return i.index < len(i.items)
}

func (i *chunkIterator[T]) Next() ([]T, error) {
	if !i.HasNext() {
		return make([]T, 0), nil
	}
	result := i.items[i.index:min(i.index+i.size, len(i.items))]
	i.index += i.size
	return result, nil
}

type mapIterator[A any, B any] struct {
	iter        ArrayIterator[A]
	transformer func(A) (B, bool, error)
}

func (m mapIterator[A, B]) HasNext() bool {
	return m.iter.HasNext()
}

func (m mapIterator[A, B]) Next() ([]B, error) {
	if !m.HasNext() {
		return make([]B, 0), nil
	}

	rows, err := m.iter.Next()
	if err != nil {
		return nil, err
	}

	result := make([]B, 0)
	for _, row := range rows {
		x, skip, err := m.transformer(row)
		if err != nil {
			return nil, err
		}
		if !skip {
			result = append(result, x)
		}
	}
	return result, nil
}

func NewMapIterator[A any, B any](iter ArrayIterator[A], transformer func(A) (B, bool, error)) mapIterator[A, B] {
	return mapIterator[A, B]{
		iter:        iter,
		transformer: transformer,
	}
}
