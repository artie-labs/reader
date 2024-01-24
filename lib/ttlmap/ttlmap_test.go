package ttlmap

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func (t *TTLMapTestSuite) TestTTLMap_Complete() {
	fp := filepath.Join(t.T().TempDir(), "test.yaml")

	store := NewMap(fp, 100*time.Millisecond, 120*time.Millisecond)
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
		store.Set(SetArgs{
			Key:   key,
			Value: key,
		}, duration)
	}

	for key := range keyToDuration {
		val, isOk := store.Get(key)
		assert.True(t.T(), isOk, fmt.Sprintf("key %s should exist", key))
		assert.Equal(t.T(), val, key)
	}

	// Now wait 50 ms.
	time.Sleep(50 * time.Millisecond)

	// foo shouldn't exist from GET, but will be still stored since GC didn't run yet.
	_, isOk := store.Get("foo")
	assert.False(t.T(), isOk, "foo")

	store.mu.Lock()
	_, isOk = store.data["foo"]
	assert.True(t.T(), isOk)
	store.mu.Unlock()

	time.Sleep(60 * time.Millisecond)

	_, isOk = store.Get("bar")
	assert.False(t.T(), isOk, "bar")
	store.mu.Lock()
	// Did the data get erased?
	for _, key := range []string{"foo", "bar"} {
		_, isOk = store.data[key]
		assert.False(t.T(), isOk, key)
	}
	store.mu.Unlock()

	_, isOk = store.Get("xyz")
	assert.True(t.T(), isOk, "xyz")
}

func (t *TTLMapTestSuite) TestFlushing() {
	// Step 1: Create a TTLMap instance with a temporary file for storage
	fp := filepath.Join(t.T().TempDir(), "test2.yaml")

	ttlMap := NewMap(fp, DefaultCleanUpInterval, DefaultFlushInterval)

	// Step 2: Add items to the map with varying DoNotFlushToDisk values
	ttlMap.Set(SetArgs{Key: "key1", Value: "value1", DoNotFlushToDisk: true}, 1*time.Hour)
	ttlMap.Set(SetArgs{Key: "key2", Value: "value2"}, 1*time.Hour)

	// Step 3: Call the flush method to save data to the file
	err := ttlMap.flush()
	assert.NoError(t.T(), err)

	// Step 4: Read the file content and check if the data is saved correctly
	content, err := os.ReadFile(fp)
	assert.NoError(t.T(), err)

	var data map[string]*ItemWrapper
	err = yaml.Unmarshal(content, &data)
	assert.NoError(t.T(), err)

	assert.Equal(t.T(), 1, len(data))
}
