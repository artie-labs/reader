package primary_key

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKeys(t *testing.T) {
	// ensure upsert doesn't mutate original arguments to `NewKeys`
	{
		keysArray := []Key{{Name: "foo", StartingValue: 20}, {Name: "bar"}}
		keys := NewKeys(keysArray)
		assert.NoError(t, keys.UpdateStartingValue("foo", "new starting value"))
		assert.Equal(t, "foo", keys.keys[0].Name)
		assert.Equal(t, "new starting value", keys.keys[0].StartingValue)
		assert.Equal(t, "foo", keysArray[0].Name)
		assert.Equal(t, 20, keysArray[0].StartingValue)
	}
}

func TestPrimaryKeys_LoadValues(t *testing.T) {
	type _tc struct {
		name           string
		startingValues []string
		endingValues   []string

		keys         []Key
		expectedKeys []Key
		expectedErr  bool
	}

	testCases := []_tc{
		{
			name:           "happy path (starting values)",
			keys:           []Key{{Name: "id"}},
			startingValues: []string{"123"},
			expectedKeys:   []Key{{Name: "id", StartingValue: "123"}},
		},
		{
			name:           "happy path w/ 2 keys (starting values)",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: []string{"123", "path:123"},
			expectedKeys:   []Key{{Name: "id", StartingValue: "123"}, {Name: "content_key", StartingValue: "path:123"}},
		},
		{
			name:         "happy path (ending values)",
			keys:         []Key{{Name: "id"}},
			endingValues: []string{"123"},
			expectedKeys: []Key{{Name: "id", EndingValue: "123"}},
		},
		{
			name:         "happy path w/ 2 keys (ending values)",
			keys:         []Key{{Name: "id"}, {Name: "content_key"}},
			endingValues: []string{"123", "path:123"},
			expectedKeys: []Key{{Name: "id", EndingValue: "123"}, {Name: "content_key", EndingValue: "path:123"}},
		},
		{
			name:           "happy path w/ 2 keys (starting and ending values)",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: []string{"122", "path:122"},
			endingValues:   []string{"123", "path:123"},
			expectedKeys:   []Key{{Name: "id", StartingValue: "122", EndingValue: "123"}, {Name: "content_key", StartingValue: "path:122", EndingValue: "path:123"}},
		},
		{
			name:           "bad data - no keys",
			keys:           []Key{},
			startingValues: []string{"123", "path:123"},
			expectedErr:    true,
		},
		{
			name:           "bad data - no values, so we just don't load",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: []string{},
			expectedErr:    false,
			expectedKeys:   []Key{{Name: "id"}, {Name: "content_key"}},
		},
	}

	for _, testCase := range testCases {
		pk := NewKeys(testCase.keys)

		err := pk.LoadValues(testCase.startingValues, testCase.endingValues)
		if testCase.expectedErr {
			assert.Error(t, err, testCase.name)
		} else {
			assert.NoError(t, err, testCase.name)
			assert.Equal(t, testCase.expectedKeys, pk.KeysList(), testCase.name)
		}

	}
}

func TestKeys_UpdateStartingValue(t *testing.T) {
	type _tc struct {
		name        string
		keys        *Keys
		keyName     string
		startingVal any

		expectedKeys []Key
		expectedErr  string
	}

	startVal2 := "Start2"

	tcs := []_tc{
		{
			name: "Key doesn't exist",
			keys: &Keys{
				keys: []Key{
					{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				},
			},
			keyName:     "Key2",
			startingVal: startVal2,
			expectedErr: "no key named Key2",
		},
		{
			name: "Update existing key",
			keys: &Keys{
				keys: []Key{
					{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				},
			},
			keyName:     "Key1",
			startingVal: startVal2,
			expectedKeys: []Key{
				{Name: "Key1", StartingValue: "Start2", EndingValue: "End1"},
			},
		},
	}

	for _, tc := range tcs {
		err := tc.keys.UpdateStartingValue(tc.keyName, tc.startingVal)
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedKeys, tc.keys.keys, tc.name)
		}
	}
}

func TestKeys_Clone(t *testing.T) {
	// empty keys
	{
		keys := NewKeys([]Key{})
		keys2 := keys.Clone()
		assert.Equal(t, keys.keys, keys2.keys)
	}
	// non-empty keys
	{
		keys := NewKeys([]Key{{Name: "foo", StartingValue: "a", EndingValue: nil}})
		keys2 := keys.Clone()
		assert.Equal(t, keys.keys, keys2.keys)
		assert.Equal(t, []Key{{"foo", "a", nil}}, keys2.keys)
	}
	// cloning actually makes a clone and doesn't reuse pointers to keys
	{
		keys := NewKeys([]Key{{Name: "foo", StartingValue: 20}, {Name: "bar", StartingValue: 0}})
		clonedKeys := keys.Clone()
		clonedKeys.UpdateStartingValue("foo", "new starting value")
		assert.Equal(t, "foo", keys.keys[0].Name)
		assert.Equal(t, 20, keys.keys[0].StartingValue)
		assert.Equal(t, "foo", clonedKeys.keys[0].Name)
		assert.Equal(t, "new starting value", clonedKeys.keys[0].StartingValue)
	}
}

func TestKeys_IsExausted(t *testing.T) {
	// empty keys
	{
		keys := NewKeys([]Key{})
		assert.True(t, keys.IsExhausted())
	}
	// one key, different starting and ending values
	{
		keys := NewKeys([]Key{{Name: "foo", StartingValue: "a", EndingValue: "b"}})
		assert.False(t, keys.IsExhausted())
	}
	// one key, same starting and ending values
	{
		keys := NewKeys([]Key{{Name: "foo", StartingValue: "a", EndingValue: "a"}})
		assert.True(t, keys.IsExhausted())
	}
	// two keys, different starting and ending values for one
	{
		keys := NewKeys([]Key{
			{Name: "foo", StartingValue: "a", EndingValue: "a"},
			{Name: "bar", StartingValue: nil, EndingValue: "a"},
		})
		assert.False(t, keys.IsExhausted())
	}
	// three keys, same starting and ending values for all
	{
		keys := NewKeys([]Key{
			{Name: "foo", StartingValue: "a", EndingValue: "a"},
			{Name: "bar", StartingValue: 2, EndingValue: 2},
			{Name: "baz", StartingValue: nil, EndingValue: nil},
		})
		assert.True(t, keys.IsExhausted())
	}
}
