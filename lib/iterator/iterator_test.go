package iterator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func errorIterator() FunctionalIterator[int] {
	return func() (int, error, bool) {
		return 0, fmt.Errorf("---==[ ERROR ]==---"), true
	}
}

func TestCollect(t *testing.T) {
	{
		// Empty iterator.
		iter := ForSlice([]int{})
		items, err := Collect(ToFunctionalIterator(iter))
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	{
		// Non-empty iterator.
		iter := ForSlice([]int{1, 2, 3, 4})
		items, err := Collect(ToFunctionalIterator(iter))
		assert.NoError(t, err)
		assert.Equal(t, items, []int{1, 2, 3, 4})
	}
	{
		// An iterator that returns an error.
		_, err := Collect(errorIterator())
		assert.ErrorContains(t, err, "---==[ ERROR ]==---")
	}
}
