package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestScanTableQuery(t *testing.T) {
	query := scanTableQuery(scanTableQueryArgs{
		Schema:       "schema",
		TableName:    "table",
		PrimaryKeys:  []string{"a", "b", "c"},
		FirstWhere:   GreaterThanEqualTo,
		StartingKeys: []string{"1", "2", "3"},
		SecondWhere:  GreaterThan,
		EndingKeys:   []string{"4", "5", "6"},
		OrderBy:      []string{"order"},
		Limit:        1,
		Columns: []schema.Column{
			{Name: "e", Type: schema.Text},
			{Name: "f", Type: schema.Int64},
			{Name: "g", Type: schema.Money}, // money will be cast
		},
	})
	assert.Equal(t, `SELECT "e","f","g"::text FROM "schema"."table" WHERE row("a","b","c") >= row(1,2,3) AND NOT row("a","b","c") > row(4,5,6) ORDER BY "order" LIMIT 1`, query)
}
