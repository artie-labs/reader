package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchIterator(t *testing.T) {
	// Length of items is 0
	{
		batches, err := Collect(Batched(ForSlice([]int{}), 2))
		assert.NoError(t, err)
		assert.Empty(t, batches)
	}
	// Length of items is 1
	{
		batches, err := Collect(Batched(ForSlice([]int{1}), 2))
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{1}}, batches)
	}
	// N is 0
	{
		batches, err := Collect(Batched(ForSlice([]int{1, 2}), 0))
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{1}, {2}}, batches)
	}
	// Length of items is a multiple of n
	{
		batches, err := Collect(Batched(ForSlice([]int{1, 2, 3, 4}), 2))
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{1, 2}, {3, 4}}, batches)
	}
	// Length of items is not a multiple of n
	{
		batches, err := Collect(Batched(ForSlice([]int{1, 2, 3, 4, 5}), 2))
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, batches)
	}
	// Error that happens immediately.
	{
		_, err := Collect(Batched(errorIterator{}, 2))
		assert.ErrorContains(t, err, "error in Next()")
	}
}
