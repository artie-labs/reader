package primary_key

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestNewKeys(t *testing.T) {
	// ensure upsert doesn't mutate original arguments to `NewKeys``
	{
		keysArray := []Key{{Name: "foo", StartingValue: 20}, {Name: "bar"}}
		keys := NewKeys(keysArray)
		keys.Upsert("foo", ptr.ToString("new starting value"), nil)
		assert.Equal(t, "foo", keys.keys[0].Name)
		assert.Equal(t, "new starting value", keys.keys[0].StartingValue)
		assert.Equal(t, 20, keysArray[0].StartingValue)
	}
}

func TestPrimaryKeys_Length(t *testing.T) {
	type _tc struct {
		name           string
		keys           *Keys
		expectedLength int
	}

	tcs := []_tc{
		{
			name:           "Nil Keys",
			keys:           nil,
			expectedLength: 0,
		},
		{
			name: "Empty Keys",
			keys: &Keys{
				keys:   []Key{},
				keyMap: map[string]bool{},
			},
			expectedLength: 0,
		},
		{
			name: "Single Key",
			keys: &Keys{
				keys: []Key{
					{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				},
				keyMap: map[string]bool{
					"Key1": true,
				},
			},
			expectedLength: 1,
		},
		{
			name: "Multiple Keys",
			keys: &Keys{
				keys: []Key{
					{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
					{Name: "Key2", StartingValue: "Start2", EndingValue: "End2"},
				},
				keyMap: map[string]bool{
					"Key1": true,
					"Key2": true,
				},
			},
			expectedLength: 2,
		},
	}

	for _, tc := range tcs {
		actualLength := tc.keys.Length()
		assert.Equal(t, tc.expectedLength, actualLength, tc.name)
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

		pk := NewKeys([]Key{})
		pk.keys = testCase.keys
		for _, key := range testCase.keys {
			pk.keyMap[key.Name] = true
		}

		err := pk.LoadValues(testCase.startingValues, testCase.endingValues)
		if testCase.expectedErr {
			assert.Error(t, err, testCase.name)
		} else {
			assert.NoError(t, err, testCase.name)
			assert.Equal(t, testCase.expectedKeys, testCase.keys, testCase.name)
		}

	}
}

func TestKeys_Upsert(t *testing.T) {
	type _tc struct {
		name        string
		keys        *Keys
		keyName     string
		startingVal *string
		endingVal   *string

		expectedKeys []Key
	}

	startVal2 := "Start2"
	endVal2 := "End2"

	tcs := []_tc{
		{
			name: "Insert new key",
			keys: &Keys{
				keys: []Key{
					{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				},
				keyMap: map[string]bool{
					"Key1": true,
				},
			},
			keyName:     "Key2",
			startingVal: &startVal2,
			endingVal:   &endVal2,
			expectedKeys: []Key{
				{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				{Name: "Key2", StartingValue: "Start2", EndingValue: "End2"},
			},
		},
		{
			name: "Update existing key (ending value only)",
			keys: &Keys{
				keys: []Key{
					{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				},
				keyMap: map[string]bool{
					"Key1": true,
				},
			},
			keyName:   "Key1",
			endingVal: &endVal2,
			expectedKeys: []Key{
				{Name: "Key1", StartingValue: "Start1", EndingValue: "End2"},
			},
		},
		{
			name: "Update existing key (starting value only)",
			keys: &Keys{
				keys: []Key{
					{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				},
				keyMap: map[string]bool{
					"Key1": true,
				},
			},
			keyName:     "Key1",
			startingVal: &startVal2,
			expectedKeys: []Key{
				{Name: "Key1", StartingValue: "Start2", EndingValue: "End1"},
			},
		},
	}

	for _, tc := range tcs {
		tc.keys.Upsert(tc.keyName, tc.startingVal, tc.endingVal)
		assert.Equal(t, tc.expectedKeys, tc.keys.keys, tc.name)
	}
}

func TestKeys_Clone(t *testing.T) {
	// empty keys
	{
		keys := NewKeys([]Key{})
		keys2 := keys.Clone()
		assert.Equal(t, keys.keys, keys2.keys)
		assert.Equal(t, keys.keyMap, keys2.keyMap)
	}
	// non-empty keys
	{
		keys := NewKeys([]Key{})
		a := "a"
		b := "b"
		keys.Upsert("foo", &a, &b)
		keys2 := keys.Clone()
		assert.Equal(t, keys.keys, keys2.keys)
		assert.Equal(t, keys.keyMap, keys2.keyMap)
		assert.Equal(t, []Key{{"foo", "a", "b"}}, keys2.keys)
		assert.Equal(t, map[string]bool{"foo": true}, keys2.keyMap)
	}
	// mutation
	{
		keys := NewKeys([]Key{{Name: "foo", StartingValue: 20}, {Name: "bar", StartingValue: 0}})
		keys2 := keys.Clone()
		keys2.Upsert("foo", ptr.ToString("new starting value"), nil)
		assert.Equal(t, "foo", keys.keys[0].Name)
		assert.Equal(t, 20, keys.keys[0].StartingValue)
		assert.Equal(t, "foo", keys2.keys[0].Name)
		assert.Equal(t, "new starting value", keys2.keys[0].StartingValue)
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
		keys := NewKeys([]Key{})
		keys.Upsert("foo", ptr.ToString("a"), ptr.ToString("b"))
		assert.False(t, keys.IsExhausted())
	}
	// one key, same starting and ending values
	{
		keys := NewKeys([]Key{})
		keys.Upsert("foo", ptr.ToString("a"), ptr.ToString("a"))
		assert.True(t, keys.IsExhausted())
	}
	// two keys, different starting and ending values for one
	{
		keys := NewKeys([]Key{})
		keys.Upsert("foo", ptr.ToString("a"), ptr.ToString("a"))
		keys.Upsert("bar", ptr.ToString(""), ptr.ToString("b"))
		assert.False(t, keys.IsExhausted())
	}
	// two keys, same starting and ending values for both
	{
		keys := NewKeys([]Key{})
		keys.Upsert("foo", ptr.ToString("a"), ptr.ToString("a"))
		keys.Upsert("bar", ptr.ToString(""), ptr.ToString(""))
		assert.True(t, keys.IsExhausted())
	}
}
