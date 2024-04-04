package kafkalib

// batched splits a slice of items into a slice of step-sized slices.
func batched[T any](items []T, step int) [][]T {
	step = max(step, 1)
	var result [][]T
	for index := 0; index < len(items); {
		end := min(index+step, len(items))
		result = append(result, items[index:end])
		index = end
	}
	return result
}
