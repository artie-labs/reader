package iterator

// Batched returns an iterator that splits a list of items into batches of the given step size.
func Batched[T any](items []T, step int) FunctionalIterator[[]T] {
	step = max(step, 1)
	var index int

	return func() ([]T, error, bool) {
		if index < len(items) {
			end := min(index+step, len(items))
			result := items[index:end]
			index = end
			return result, nil, true
		}
		return nil, nil, false
	}
}
