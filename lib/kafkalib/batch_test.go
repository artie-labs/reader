package kafkalib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatched(t *testing.T) {
	// length of items is 0
	{
		batches := batched([]int{}, 2)
		assert.Empty(t, batches)
	}
	// length of items is 1
	{
		batches := batched([]int{1}, 2)
		assert.Equal(t, [][]int{{1}}, batches)
	}
	// n is 0
	{
		batches := batched([]int{1, 2}, 0)
		assert.Equal(t, [][]int{{1}, {2}}, batches)
	}
	// length of items is a multiple of n
	{
		batches := batched([]int{1, 2, 3, 4}, 2)
		assert.Equal(t, [][]int{{1, 2}, {3, 4}}, batches)
	}
	// length of items is not a multiple of n
	{
		batches := batched([]int{1, 2, 3, 4, 5}, 2)
		assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, batches)
	}
}
