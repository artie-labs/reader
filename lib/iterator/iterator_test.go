package iterator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type errorIterator struct {
	errorAfter int
}

func (errorIterator) HasNext() bool { return true }

func (ei *errorIterator) Next() ([]int, error) {
	if ei.errorAfter <= 0 {
		return nil, fmt.Errorf("error in Next()")
	}
	ei.errorAfter--
	return []int{ei.errorAfter}, nil
}

func TestCollect(t *testing.T) {
	{
		// Empty iterator.
		iter := ForSlice([]int{})
		items, err := Collect(iter)
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	{
		// Non-empty iterator.
		iter := ForSlice([]int{1, 2, 3, 4})
		items, err := Collect(iter)
		assert.NoError(t, err)
		assert.Equal(t, items, []int{1, 2, 3, 4})
	}
	{
		// When [Iterator.Next] returns an error immedately.
		_, err := Collect(&errorIterator{})
		assert.ErrorContains(t, err, "error in Next()")
	}
	{
		// When [Iterator.Next] returns an error after several calls.
		_, err := Collect(&errorIterator{errorAfter: 5})
		assert.ErrorContains(t, err, "error in Next()")
	}
}

func TestFlatten(t *testing.T) {
	{
		// Empty iterator.
		iter := ForSlice([][]int{})
		items, err := Collect(Flatten(iter))
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	{
		// One empty batch.
		iter := ForSlice([][]int{{}})
		items, err := Collect(Flatten(iter))
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	{
		// Two empty batches.
		iter := ForSlice([][]int{{}, {}})
		items, err := Collect(Flatten(iter))
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	{
		// One non-empty batch with a single item.
		iter := ForSlice([][]int{{6}})
		items, err := Collect(Flatten(iter))
		assert.NoError(t, err)
		assert.Equal(t, items, []int{6})
	}
	{
		// One non-empty batch with multiple items.
		iter := ForSlice([][]int{{6, 7, 8, 9}})
		items, err := Collect(Flatten(iter))
		assert.NoError(t, err)
		assert.Equal(t, items, []int{6, 7, 8, 9})
	}
	{
		// Two non-empty batches with multiple items.
		iter := ForSlice([][]int{{6, 7, 8, 9}, {1, 2, 3}})
		items, err := Collect(Flatten(iter))
		assert.NoError(t, err)
		assert.Equal(t, items, []int{6, 7, 8, 9, 1, 2, 3})
	}
	{
		// Empty and non-empty batches with differing amounts of items.
		iter := ForSlice([][]int{{}, {6, 7, 8, 9}, {}, {1, 2, 3}, {4}, {}, {}, {3, 2}, {}})
		items, err := Collect(Flatten(iter))
		assert.NoError(t, err)
		assert.Equal(t, items, []int{6, 7, 8, 9, 1, 2, 3, 4, 3, 2})
	}
	{
		// When [Iterator.Next] returns an error immediately.
		_, err := Collect(Flatten(&errorIterator{}))
		assert.ErrorContains(t, err, "error in Next()")
	}
	{
		// When [Iterator.Next] returns an error after several successful calls.
		_, err := Collect(Flatten(&errorIterator{errorAfter: 10}))
		assert.ErrorContains(t, err, "error in Next()")
	}
}
