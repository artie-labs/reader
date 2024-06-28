package persistedmap

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"os"
	"testing"
)

func TestPersistedMap_LoadFromFile(t *testing.T) {
	tmpFile, err := os.Create(fmt.Sprintf("%s/foo", t.TempDir()))
	assert.NoError(t, err)

	// Write initial data to the file
	initialData := map[string]any{"key1": "value1", "key2": 2}
	yamlBytes, err := yaml.Marshal(initialData)
	assert.NoError(t, err)
	_, err = tmpFile.Write(yamlBytes)
	assert.NoError(t, err)
	tmpFile.Close()

	// Load the data from the file into PersistedMap
	pMap := NewPersistedMap(tmpFile.Name())
	pMap.mu.Lock()
	defer pMap.mu.Unlock()
	assert.Equal(t, initialData, pMap.data)
}

func TestPersistedMap_Flush(t *testing.T) {
	tmpFile := fmt.Sprintf("%s/persistedmap_test", t.TempDir())

	pMap := NewPersistedMap(tmpFile)
	pMap.Set("key1", "value1")
	pMap.Set("key2", 2)

	assert.NoError(t, pMap.flush())

	// Does the data exist?
	val, isOk := pMap.Get("key1")
	assert.True(t, isOk)
	assert.Equal(t, "value1", val)

	val, isOk = pMap.Get("key2")
	assert.Equal(t, 2, val)
	assert.True(t, isOk)

	// If I load a new PersistedMap, does it come back?
	pMap2 := NewPersistedMap(tmpFile)
	val, isOk = pMap2.Get("key1")
	assert.True(t, isOk)
	assert.Equal(t, "value1", val)

	val, isOk = pMap2.Get("key2")
	assert.Equal(t, 2, val)
	assert.True(t, isOk)
}
