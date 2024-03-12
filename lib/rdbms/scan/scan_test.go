package scan

import (
	"fmt"
	"testing"

	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/stretchr/testify/assert"
)

type mockAdapter struct {
	returnError bool
}

func (m mockAdapter) ParsePrimaryKeyValue(columnName string, value string) (any, error) {
	if m.returnError {
		return nil, fmt.Errorf("mock error in ParsePrimaryKeyValue")
	} else {
		return fmt.Sprintf("parsed-%s-%s", columnName, value), nil
	}
}

func (mockAdapter) BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any, error) {
	panic("not implemented")
}

func (mockAdapter) ParseRow(row []any) (map[string]any, error) {
	panic("not implemented")
}

func TestParsePkValueOverrides(t *testing.T) {
	{
		// Empty values.
		result, err := parsePkValueOverrides([]string{}, []primary_key.Key{}, mockAdapter{})
		assert.NoError(t, err)
		assert.Empty(t, result)
	}
	{
		// Non-empty values + empty primary keys
		_, err := parsePkValueOverrides([]string{"foo"}, []primary_key.Key{}, mockAdapter{})
		assert.ErrorContains(t, err, "keys (0), and passed in values (1) length does not match, keys: [], values: [foo]")
	}
	{
		// len(values) != len(primary keys)
		_, err := parsePkValueOverrides([]string{"123", "456"}, []primary_key.Key{{Name: "foo"}}, mockAdapter{})
		assert.ErrorContains(t, err, "keys (1), and passed in values (2) length does not match, keys: [{foo <nil> <nil>}]")
	}
	{
		// len(values) == len(primary keys) + error in ParsePrimaryKeyValue
		adapter := mockAdapter{returnError: true}
		_, err := parsePkValueOverrides([]string{"123", "456"}, []primary_key.Key{{Name: "foo"}, {Name: "bar"}}, adapter)
		assert.ErrorContains(t, err, "failed to parse value '123': mock error in ParsePrimaryKeyValue")
	}
	{
		// Happy path: len(values) == len(primary keys) + no error
		adapter := mockAdapter{returnError: false}
		result, err := parsePkValueOverrides([]string{"123", "456"}, []primary_key.Key{{Name: "foo"}, {Name: "bar"}}, adapter)
		assert.NoError(t, err)
		assert.Equal(t, []any{"parsed-foo-123", "parsed-bar-456"}, result)
	}
}
