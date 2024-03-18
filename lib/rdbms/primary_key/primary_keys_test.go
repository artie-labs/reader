package primary_key

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKeys(t *testing.T) {
	// ensure upsert doesn't mutate original arguments to `NewKeys`
	keysArray := []Key{{Name: "foo", StartingValue: 20}, {Name: "bar"}}
	keys := NewKeys(keysArray)
	changed, err := keys.UpdateStartingValue("foo", "new starting value")
	assert.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, "foo", keys.keys[0].Name)
	assert.Equal(t, "new starting value", keys.keys[0].StartingValue)
	assert.Equal(t, "foo", keysArray[0].Name)
	assert.Equal(t, 20, keysArray[0].StartingValue)
}

func TestPrimaryKeys_LoadValues(t *testing.T) {
	type _tc struct {
		name           string
		startingValues []any
		endingValues   []any

		keys         []Key
		expectedKeys []Key
		expectedErr  bool
	}

	testCases := []_tc{
		{
			name:           "happy path (starting values)",
			keys:           []Key{{Name: "id"}},
			startingValues: []any{"123"},
			expectedKeys:   []Key{{Name: "id", StartingValue: "123"}},
		},
		{
			name:           "happy path w/ 2 keys (starting values)",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: []any{"123", "path:123"},
			expectedKeys:   []Key{{Name: "id", StartingValue: "123"}, {Name: "content_key", StartingValue: "path:123"}},
		},
		{
			name:         "happy path (ending values)",
			keys:         []Key{{Name: "id"}},
			endingValues: []any{"123"},
			expectedKeys: []Key{{Name: "id", EndingValue: "123"}},
		},
		{
			name:         "happy path w/ 2 keys (ending values)",
			keys:         []Key{{Name: "id"}, {Name: "content_key"}},
			endingValues: []any{"123", "path:123"},
			expectedKeys: []Key{{Name: "id", EndingValue: "123"}, {Name: "content_key", EndingValue: "path:123"}},
		},
		{
			name:           "happy path w/ 2 keys (starting and ending values)",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: []any{"122", "path:122"},
			endingValues:   []any{"123", "path:123"},
			expectedKeys:   []Key{{Name: "id", StartingValue: "122", EndingValue: "123"}, {Name: "content_key", StartingValue: "path:122", EndingValue: "path:123"}},
		},
		{
			name:           "bad data - no keys",
			keys:           []Key{},
			startingValues: []any{"123", "path:123"},
			expectedErr:    true,
		},
		{
			name:           "bad data - no values, so we just don't load",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: []any{},
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
			assert.Equal(t, testCase.expectedKeys, pk.Keys(), testCase.name)
		}

	}
}

func TestKeys_UpdateStartingValue(t *testing.T) {
	type _tc struct {
		name        string
		keys        []Key
		keyName     string
		startingVal any

		expectedChanged bool
		expectedKeys    []Key
		expectedErr     string
	}

	startVal2 := "Start2"

	tcs := []_tc{
		{
			name: "Key doesn't exist",
			keys: []Key{
				{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
			},
			keyName:     "Key2",
			startingVal: startVal2,
			expectedErr: "no key named Key2",
		},
		{
			name: "Update existing key with existing start value",
			keys: []Key{
				{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				{Name: "Key2", StartingValue: 2, EndingValue: 2},
			},
			keyName:         "Key1",
			startingVal:     "Start1",
			expectedChanged: false,
			expectedKeys: []Key{
				{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
				{Name: "Key2", StartingValue: 2, EndingValue: 2},
			},
		},
		{
			name: "Update existing key with new value",
			keys: []Key{
				{Name: "Key1", StartingValue: "Start1", EndingValue: "End1"},
			},
			keyName:         "Key1",
			startingVal:     startVal2,
			expectedChanged: true,
			expectedKeys: []Key{
				{Name: "Key1", StartingValue: startVal2, EndingValue: "End1"},
			},
		},
	}

	for _, tc := range tcs {
		keys := &Keys{tc.keys}
		changed, err := keys.UpdateStartingValue(tc.keyName, tc.startingVal)
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedKeys, keys.keys, tc.name)
			assert.Equal(t, tc.expectedChanged, changed)
		}
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
	// five keys, same starting and ending values for all
	{
		keys := NewKeys([]Key{
			{Name: "foo", StartingValue: "a", EndingValue: "a"},
			{Name: "bar", StartingValue: 2, EndingValue: 2},
			{Name: "qux", StartingValue: []byte{}, EndingValue: []byte{}},
			{Name: "quxx", StartingValue: []byte{byte(20)}, EndingValue: []byte{byte(20)}},
			{Name: "baz", StartingValue: nil, EndingValue: nil},
		})
		assert.True(t, keys.IsExhausted())
	}
	// three keys, second has different values
	{
		keys := NewKeys([]Key{
			{Name: "foo", StartingValue: "a", EndingValue: "a"},
			{Name: "qux", StartingValue: []byte{byte(0)}, EndingValue: []byte{byte(1)}},
			{Name: "baz", StartingValue: nil, EndingValue: nil},
		})
		assert.False(t, keys.IsExhausted())
	}
	// three keys, second has different values
	{
		keys := NewKeys([]Key{
			{Name: "foo", StartingValue: "a", EndingValue: "a"},
			{Name: "qux", StartingValue: []byte{byte(10)}, EndingValue: "string"},
			{Name: "baz", StartingValue: nil, EndingValue: nil},
		})
		assert.False(t, keys.IsExhausted())
	}
}

func TestEqual(t *testing.T) {
	// string
	assert.True(t, equal("a", "a"))
	assert.False(t, equal("a", "b"))

	// int
	assert.True(t, equal(123, 123))
	assert.False(t, equal(124, 123))

	// []byte
	assert.True(t, equal([]byte{}, []byte{}))
	assert.True(t, equal([]byte{byte(1)}, []byte{byte(1)}))
	assert.False(t, equal([]byte{}, nil))
	assert.False(t, equal([]byte{}, ""))
	assert.False(t, equal(nil, []byte{}))
	assert.False(t, equal("", []byte{}))
	assert.False(t, equal([]byte{byte(1)}, []byte{byte(12)}))
}
