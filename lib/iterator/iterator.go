package iterator

import "fmt"

type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
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

type flatIterator[T any] struct {
	iter Iterator[[]T]

	cur      []T
	curIndex int

	err error
}

func (fi *flatIterator[T]) seek() bool {
	fi.curIndex = 0
	fi.cur = nil

	for fi.iter.HasNext() {
		items, err := fi.iter.Next()
		if err != nil {
			fi.err = err
			return true
		} else if len(items) > 0 {
			fi.cur = items
			return true
		}
	}

	return false
}

func (fi *flatIterator[T]) HasNext() bool {
	if fi.curIndex < len(fi.cur) {
		return true
	}
	return fi.seek()
}

func (fi *flatIterator[T]) Next() (T, error) {
	if !fi.HasNext() {
		var unused T
		return unused, fmt.Errorf("iterator has finished")
	}

	if fi.err != nil {
		var unused T
		return unused, fi.err
	}

	value := fi.cur[fi.curIndex]
	fi.curIndex++
	return value, nil
}

// Flatten takes an [Iterator] of slices and produces new iterator over all the items in all the slices.
func Flatten[T any](iter Iterator[[]T]) Iterator[T] {
	return &flatIterator[T]{iter: iter}
}
