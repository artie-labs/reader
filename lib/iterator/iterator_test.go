package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
