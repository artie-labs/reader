package lib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SingleBatchIterator(t *testing.T) {
	{
		// Empty batch
		iter := NewSingleBatchIterator([]string{})
		assert.True(t, iter.HasNext())
		batch, err := iter.Next()
		assert.NoError(t, err)
		assert.Empty(t, batch)
		assert.False(t, iter.HasNext())
		_, err = iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	{
		// Non-empty batch
		iter := NewSingleBatchIterator([]string{"a", "b", "c", "d"})
		assert.True(t, iter.HasNext())
		batch, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c", "d"}, batch)
		assert.False(t, iter.HasNext())
		_, err = iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
}

func Test_BatchIterator(t *testing.T) {
	{
		// No batches
		iter := NewBatchIterator([][]string{})
		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	{
		// One empty batch
		iter := NewBatchIterator([][]string{{}})
		assert.True(t, iter.HasNext())
		batch, err := iter.Next()
		assert.NoError(t, err)
		assert.Empty(t, batch)
		assert.False(t, iter.HasNext())
		_, err = iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
	{
		// Two non-empty batches one empty batch
		iter := NewBatchIterator([][]string{{"a", "b"}, {}, {"c", "d"}})
		assert.True(t, iter.HasNext())
		{
			batch, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []string{"a", "b"}, batch)
		}

		assert.True(t, iter.HasNext())
		{
			batch, err := iter.Next()
			assert.NoError(t, err)
			assert.Empty(t, batch)
		}

		assert.True(t, iter.HasNext())
		{
			batch, err := iter.Next()
			assert.NoError(t, err)
			assert.Equal(t, []string{"c", "d"}, batch)
		}

		assert.False(t, iter.HasNext())
		_, err := iter.Next()
		assert.ErrorContains(t, err, "iterator has finished")
	}
}
