package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceIterator(t *testing.T) {
	{
		// No items
		iter := FromSlice([][]string{})
		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	{
		// One empty slice
		iter := FromSlice([][]string{{}})
		assert.True(t, iter.HasNext())
		item, err := iter.Next()
		assert.NoError(t, err)
		assert.Empty(t, item)
		assert.False(t, iter.HasNext())
		_, err = iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	{
		// Two non-empty slices + one empty slice
		iter := FromSlice([][]string{{"a", "b"}, {}, {"c", "d"}})
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
	iter := Once(103)
	assert.True(t, iter.HasNext())
	item, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, 103, item)
	assert.False(t, iter.HasNext())
	_, err = iter.Next()
	assert.ErrorContains(t, err, "iterator has finished")
}
