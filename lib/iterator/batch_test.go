package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchIterator(t *testing.T) {
	// length of items is 0
	{
		batches, err := Collect(Batched(ForSlice([]int{}), 2))
		assert.NoError(t, err)
		assert.Empty(t, batches)
	}
	// length of items is 1
	{
		batches, err := Collect(Batched(ForSlice([]int{1}), 2))
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{1}}, batches)
	}
	// n is 0
	{
		batches, err := Collect(Batched(ForSlice([]int{1, 2}), 0))
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{1}, {2}}, batches)
	}
	// length of items is a multiple of n
	{
		batches, err := Collect(Batched(ForSlice([]int{1, 2, 3, 4}), 2))
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{1, 2}, {3, 4}}, batches)
	}
	// length of items is not a multiple of n
	{
		batches, err := Collect(Batched(ForSlice([]int{1, 2, 3, 4, 5}), 2))
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, batches)
	}
}
