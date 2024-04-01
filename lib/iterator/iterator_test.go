package iterator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type errorIterator struct{}

func (errorIterator) HasNext() bool { return true }

func (errorIterator) Next() (int, error) { return 0, fmt.Errorf("error in Next()") }

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
		// When [Iterator.Next] throws an error.
		_, err := Collect(errorIterator{})
		assert.ErrorContains(t, err, "error in Next()")
	}
}
