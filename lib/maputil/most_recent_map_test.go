package maputil

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMostRecentMap(t *testing.T) {
	{
		// Test case #1
		mre := NewMostRecentMap[string]()
		mre.AddItem(0, "hello")
		{
			// Get ts = -5
			item, found := mre.GetItem(-5)
			assert.False(t, found)
			assert.Equal(t, "", item)
		}
		{
			// Get ts = 0
			item, found := mre.GetItem(0)
			assert.True(t, found)
			assert.Equal(t, "hello", item)
		}
		{
			// Get ts = 5
			item, found := mre.GetItem(5)
			assert.True(t, found)
			assert.Equal(t, "hello", item)
		}

		// Put more
		mre.AddItem(6, "world")
		{
			// Get ts = 3
			item, found := mre.GetItem(3)
			assert.True(t, found)
			assert.Equal(t, "hello", item)
		}
		{
			// Get ts = 6
			item, found := mre.GetItem(6)
			assert.True(t, found)
			assert.Equal(t, "world", item)
		}
		{
			item, found := mre.GetItem(8888)
			assert.True(t, found)
			assert.Equal(t, "world", item)
		}
	}
	{
		// Test case #2
		mre := NewMostRecentMap[string]()
		mre.AddItem(6, "artie")
		{
			// ts = 3
			item, found := mre.GetItem(3)
			assert.False(t, found)
			assert.Equal(t, "", item)
		}
		{
			// ts = 6
			item, found := mre.GetItem(6)
			assert.True(t, found)
			assert.Equal(t, "artie", item)
		}
		{
			// ts = 9000
			item, found := mre.GetItem(9000)
			assert.True(t, found)
			assert.Equal(t, "artie", item)
		}
	}
}
