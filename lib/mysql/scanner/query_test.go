package scanner

import (
	"testing"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestSqlPlaceholders(t *testing.T) {
	assert.Equal(t, []string{}, sqlPlaceholders(0))
	assert.Equal(t, []string{"?"}, sqlPlaceholders(1))
	assert.Equal(t, []string{"?", "?"}, sqlPlaceholders(2))
}

func TestBuildScanTableQuery(t *testing.T) {
	keys := primary_key.NewKeys()
	keys.Upsert("foo", ptr.ToString("a"), ptr.ToString("b"))
	{
		// uninclusive upper and lower bounds
		query, parameters, err := buildScanTableQuery(buildScanTableQueryArgs{
			TableName:   "table",
			PrimaryKeys: keys,
			Columns: []schema.Column{
				{Name: "foo"},
				{Name: "bar"},
			},
			Limit: 12,
		})
		assert.NoError(t, err)
		assert.Equal(t, "SELECT `foo`,`bar` FROM `table` WHERE (`foo`) > (?) AND NOT (`foo`) >= (?) ORDER BY `foo` LIMIT 12", query)
		assert.Equal(t, []interface{}{"a", "b"}, parameters)
	}
	{
		// inclusive upper and lower bounds
		query, parameters, err := buildScanTableQuery(buildScanTableQueryArgs{
			TableName:   "table",
			PrimaryKeys: keys,
			Columns: []schema.Column{
				{Name: "foo"},
				{Name: "bar"},
			},
			InclusiveLowerBound: true,
			InclusiveUpperBound: true,
			Limit:               12,
		})
		assert.NoError(t, err)
		assert.Equal(t, "SELECT `foo`,`bar` FROM `table` WHERE (`foo`) >= (?) AND NOT (`foo`) > (?) ORDER BY `foo` LIMIT 12", query)
		assert.Equal(t, []interface{}{"a", "b"}, parameters)
	}
}
