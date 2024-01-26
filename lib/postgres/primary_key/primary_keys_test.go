package primary_key

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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
		startingValues string
		endingValues   string

		keys         []Key
		expectedKeys []Key
		expectedErr  bool
	}

	testCases := []_tc{
		{
			name:           "happy path (starting values)",
			keys:           []Key{{Name: "id"}},
			startingValues: "123",
			expectedKeys:   []Key{{Name: "id", StartingValue: "123"}},
		},
		{
			name:           "happy path w/ 2 keys (starting values)",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: "123,path:123",
			expectedKeys:   []Key{{Name: "id", StartingValue: "123"}, {Name: "content_key", StartingValue: "path:123"}},
		},
		{
			name:         "happy path (ending values)",
			keys:         []Key{{Name: "id"}},
			endingValues: "123",
			expectedKeys: []Key{{Name: "id", EndingValue: "123"}},
		},
		{
			name:         "happy path w/ 2 keys (ending values)",
			keys:         []Key{{Name: "id"}, {Name: "content_key"}},
			endingValues: "123,path:123",
			expectedKeys: []Key{{Name: "id", EndingValue: "123"}, {Name: "content_key", EndingValue: "path:123"}},
		},
		{
			name:           "happy path w/ 2 keys (starting and ending values)",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: "122,path:122",
			endingValues:   "123,path:123",
			expectedKeys:   []Key{{Name: "id", StartingValue: "122", EndingValue: "123"}, {Name: "content_key", StartingValue: "path:122", EndingValue: "path:123"}},
		},
		{
			name:           "bad data - no keys",
			keys:           []Key{},
			startingValues: "123,path:123",
			expectedErr:    true,
		},
		{
			name:           "bad data - no values, so we just don't load",
			keys:           []Key{{Name: "id"}, {Name: "content_key"}},
			startingValues: "",
			expectedErr:    false,
			expectedKeys:   []Key{{Name: "id"}, {Name: "content_key"}},
		},
	}

	for _, testCase := range testCases {

		pk := NewKeys()
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
