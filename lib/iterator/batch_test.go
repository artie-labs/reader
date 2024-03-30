package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchIterator(t *testing.T) {
	// length of items is 0
	{
		iter := NewBatchIterator([]int{}, 2)
		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	// length of items is 1
	{
		iter := NewBatchIterator([]int{1}, 2)
		assert.True(t, iter.HasNext())
		{
			items, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []int{1}, items)
			assert.Equal(t, 1, (iter.(*batchIterator[int])).index)
		}
		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	// n is 0
	{
		iter := NewBatchIterator([]int{1, 2}, 0)
		assert.True(t, iter.HasNext())
		{
			items, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []int{1}, items)
		}
		{
			items, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []int{2}, items)
		}
		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	// length of items is a multiple of n
	{
		iter := NewBatchIterator([]int{1, 2, 3, 4}, 2)
		assert.True(t, iter.HasNext())
		{
			items, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []int{1, 2}, items)
			assert.Equal(t, 2, (iter.(*batchIterator[int])).index)
		}
		assert.True(t, iter.HasNext())
		{
			items, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []int{3, 4}, items)
			assert.Equal(t, 4, (iter.(*batchIterator[int])).index)
		}
		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	// length of items is not a multiple of n
	{
		iter := NewBatchIterator([]int{1, 2, 3, 4, 5}, 2)
		assert.True(t, iter.HasNext())
		{
			items, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []int{1, 2}, items)
			assert.Equal(t, 2, (iter.(*batchIterator[int])).index)
		}
		assert.True(t, iter.HasNext())
		{
			items, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []int{3, 4}, items)
			assert.Equal(t, 4, (iter.(*batchIterator[int])).index)
		}
		assert.True(t, iter.HasNext())
		{
			items, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []int{5}, items)
		}
		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
}
