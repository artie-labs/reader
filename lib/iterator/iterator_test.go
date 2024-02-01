package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type NoopIterator[T int] struct{}

func (m NoopIterator[T]) HasNext() bool {
	return false
}

func (m NoopIterator[T]) Next() []int {
	panic("should not be called")
}

func TestBatchIterator(t *testing.T) {
	iter := NewBatchIterator([]int{1, 2, 3, 4, 5}, 2)
	assert.True(t, iter.HasNext())
	{
		items := iter.Next()
		assert.Equal(t, []int{1, 2}, items)
		assert.Equal(t, 2, iter.index)
	}
	assert.True(t, iter.HasNext())
	{
		assert.Equal(t, []int{3, 4}, iter.Next())
		assert.Equal(t, 4, iter.index)
	}
	assert.True(t, iter.HasNext())
	{
		assert.Equal(t, []int{5}, iter.Next())
	}
	assert.False(t, iter.HasNext())
}

func TestCollect(t *testing.T) {
	// happy path
	{
		iter := NewBatchIterator([]int{1, 2, 3, 4, 5}, 3)
		assert.Equal(t, []int{1, 2, 3, 4, 5}, Collect(iter))
	}
	// empty - one chunk that is empty
	{
		iter := NewBatchIterator([]int{}, 3)
		assert.Empty(t, Collect(iter))
	}
	// empty - zero chunks
	{
		iter := NoopIterator[int]{}
		assert.Empty(t, Collect(iter))
	}
}
