package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceIterator(t *testing.T) {
	{
		// No items
		iter := ForSlice([][]string{})
		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	{
		// One empty slice
		items, err := Collect(ToFunctionalIterator(ForSlice([]string{})))
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	{
		// Two non-empty slices + one empty slice
		iter := ForSlice([][]string{{"a", "b"}, {}, {"c", "d"}})
		assert.True(t, iter.HasNext())
		{
			item, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []string{"a", "b"}, item)
		}

		assert.True(t, iter.HasNext())
		{
			item, err := iter.Next()
			assert.NoError(t, err)
			assert.Empty(t, item)
		}

		assert.True(t, iter.HasNext())
		{
			item, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []string{"c", "d"}, item)
		}

		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
}

func TestOnce(t *testing.T) {
	items, err := Collect(ToFunctionalIterator(Once(103)))
	assert.NoError(t, err)
	assert.Equal(t, []int{103}, items)
}
