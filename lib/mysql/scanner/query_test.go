package scanner

import (
	"testing"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/stretchr/testify/assert"
)

func TestQueryPlaceholders(t *testing.T) {
	assert.Equal(t, []string{}, queryPlaceholders(0))
	assert.Equal(t, []string{"?"}, queryPlaceholders(1))
	assert.Equal(t, []string{"?", "?"}, queryPlaceholders(2))
}

func TestBuildScanTableQuery(t *testing.T) {
	keys := []primary_key.Key{
		{Name: "foo", StartingValue: "a", EndingValue: "b"},
	}
	{
		// exclusive lower bound
		query, parameters, err := buildScanTableQuery(
			"table",
			keys,
			[]schema.Column{
				{Name: "foo"},
				{Name: "bar"},
			},
			false,
			12,
		)
		assert.NoError(t, err)
		assert.Equal(t, "SELECT `foo`,`bar` FROM `table` WHERE (`foo`) > (?) AND (`foo`) <= (?) ORDER BY `foo` LIMIT 12", query)
		assert.Equal(t, []any{"a", "b"}, parameters)
	}
	{
		// inclusive upper and lower bounds
		query, parameters, err := buildScanTableQuery(
			"table",
			keys,
			[]schema.Column{
				{Name: "foo"},
				{Name: "bar"},
			},
			true,
			12,
		)
		assert.NoError(t, err)
		assert.Equal(t, "SELECT `foo`,`bar` FROM `table` WHERE (`foo`) >= (?) AND (`foo`) <= (?) ORDER BY `foo` LIMIT 12", query)
		assert.Equal(t, []any{"a", "b"}, parameters)
	}
}
