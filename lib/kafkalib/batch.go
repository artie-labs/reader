package kafkalib

import "slices"

// batched splits a slice of items into a slice of step-sized slices.
func batched[T any](items []T, step int) [][]T {
	return slices.Collect(slices.Chunk(items, max(step, 1)))
}
