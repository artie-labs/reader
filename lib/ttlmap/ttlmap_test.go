package ttlmap

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"time"
)

func (t *TTLMapTestSuite) TestTTLMap_Complete() {
	fp := "/tmp/test.yaml"
	assert.NoError(t.T(), os.RemoveAll(fp))
	defer os.RemoveAll(fp)

	store := NewMap(t.ctx, fp, 50*time.Millisecond, 100*time.Millisecond)
	keyToDuration := map[string]time.Duration{
		"foo": 50 * time.Millisecond,
		"bar": 100 * time.Millisecond,
		"baz": 150 * time.Millisecond,
		"xyz": 2 * time.Second,
		"123": 5 * time.Second,
	}

	for key := range keyToDuration {
		_, isOk := store.Get(key)
		assert.False(t.T(), isOk, fmt.Sprintf("key %s should not exist", key))
	}

	// Now, insert all of this and then wait 100 ms.
	for key, duration := range keyToDuration {
		store.Set(key, key, duration)
	}

	for key := range keyToDuration {
		val, isOk := store.Get(key)
		assert.True(t.T(), isOk, fmt.Sprintf("key %s should exist", key))
		assert.Equal(t.T(), val, key)
	}

	// Now wait 100 ms.
	time.Sleep(100 * time.Millisecond)
	_, isOk := store.Get("bar")
	assert.False(t.T(), isOk, "bar")

	_, isOk = store.Get("xyz")
	assert.True(t.T(), isOk, "xyz")
}
