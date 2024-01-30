package collections

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type NoopIterator[T int] struct{}

func (m NoopIterator[T]) HasNext() bool {
	return false
}

func (m NoopIterator[T]) Next() ([]int, error) {
	panic("should not be called")
}

type ErrorIterator[T int] struct{}

func (m ErrorIterator[T]) HasNext() bool {
	return true
}

func (m ErrorIterator[T]) Next() ([]int, error) {
	return nil, fmt.Errorf("mock error")
}

func TestChunkIterator(t *testing.T) {
	iter := NewChunkIterator([]int{1, 2, 3, 4, 5}, 2)
	assert.True(t, iter.HasNext())
	{
		items, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 2}, items)
		assert.Equal(t, 2, iter.index)
	}
	assert.True(t, iter.HasNext())
	{
		items, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, []int{3, 4}, items)
	}
	assert.True(t, iter.HasNext())
	{
		items, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, []int{5}, items)
	}
	assert.False(t, iter.HasNext())
}

func TestCollect(t *testing.T) {
	// happy path
	{
		iter := NewChunkIterator([]int{1, 2, 3, 4, 5}, 3)
		items, err := Collect(iter)
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3, 4, 5}, items)
	}
	// empty - one chunk that is empty
	{
		iter := NewChunkIterator([]int{}, 3)
		items, err := Collect(iter)
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	// empty - zero chunks
	{
		iter := NoopIterator[int]{}
		items, err := Collect(iter)
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	// error handling
	{
		iter := ErrorIterator[int]{}
		_, err := Collect(iter)
		assert.ErrorContains(t, err, "mock error")
	}
}

func TestMapIterator(t *testing.T) {
	// happy path
	{
		iter := NewChunkIterator([]int{1, 2, 3, 4, 5}, 2)
		iter2 := NewMapIterator(iter, func(a int) (string, bool, error) { return fmt.Sprintf("%d*2=%d", a, a*2), false, nil })
		result, err := Collect(iter2)
		assert.NoError(t, err)
		assert.False(t, iter.HasNext())
		assert.False(t, iter2.HasNext())
		assert.Equal(t, []string{"1*2=2", "2*2=4", "3*2=6", "4*2=8", "5*2=10"}, result)
	}
	// happy path - with filter skipping even numbers
	{
		iter := NewChunkIterator([]int{1, 2, 3, 4, 5}, 2)
		iter2 := NewMapIterator(iter, func(a int) (string, bool, error) { return fmt.Sprintf("%d*2=%d", a, a*2), a%2 == 0, nil })
		result, err := Collect(iter2)
		assert.NoError(t, err)
		assert.False(t, iter.HasNext())
		assert.False(t, iter2.HasNext())
		assert.Equal(t, []string{"1*2=2", "3*2=6", "5*2=10"}, result)
	}
	// empty - one chunk that is empty
	{
		iter := NewChunkIterator([]int{}, 3)
		iter2 := NewMapIterator(iter, func(a int) (string, bool, error) { assert.Fail(t, "should not be called"); return "", false, nil })
		items, err := Collect(iter2)
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	// empty - zero chunks
	{
		iter := NoopIterator[int]{}
		iter2 := NewMapIterator(iter, func(a int) (string, bool, error) { assert.Fail(t, "should not be called"); return "", false, nil })
		items, err := Collect(iter2)
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	// error handling - parent iterator error
	{
		iter := ErrorIterator[int]{}
		iter2 := NewMapIterator(iter, func(a int) (string, bool, error) { assert.Fail(t, "should not be called"); return "", false, nil })
		_, err := Collect(iter2)
		assert.ErrorContains(t, err, "mock error")
	}
	// error handling - map function error
	{
		iter := NewChunkIterator([]int{1, 2, 3, 4, 5}, 2)
		iter2 := NewMapIterator(iter, func(a int) (string, bool, error) { return "", false, fmt.Errorf("map func error") })
		_, err := Collect(iter2)
		assert.ErrorContains(t, err, "map func error")
	}
}
