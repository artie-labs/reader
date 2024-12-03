package persistedlist

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestPersistedList(t *testing.T) {
	type Dog struct {
		Name  string `json:"name"`
		Breed string `json:"breed"`
	}

	fp, err := os.MkdirTemp(os.TempDir(), "*")
	assert.NoError(t, err)

	pl := NewPersistedList[Dog](filepath.Join(fp, "dogs.json"))
	// Now, let's load a bunch of dogs
	dogs := []Dog{
		{Name: "Buddy", Breed: "Golden Retriever"},
		{Name: "Bella", Breed: "Labrador Retriever"},
		{Name: "Max", Breed: "German Shepherd"},
		{Name: "Dusty", Breed: "Mini Australian Shepherd"},
	}

	for _, dog := range dogs {
		assert.NoError(t, pl.Push(dog))
	}

	// Now, let's get the data
	assert.Equal(t, dogs, pl.GetData())
}
