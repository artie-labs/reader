package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdms/primary_key"
	"github.com/artie-labs/transfer/lib/ptr"
)

func TestScanTableQuery(t *testing.T) {
	primaryKeys := primary_key.NewKeys()
	primaryKeys.Upsert("a", ptr.ToString("1"), ptr.ToString("4"))
	primaryKeys.Upsert("b", ptr.ToString("2"), ptr.ToString("5"))
	primaryKeys.Upsert("c", ptr.ToString("3"), ptr.ToString("6"))

	query := scanTableQuery(scanTableQueryArgs{
		Schema:      "schema",
		TableName:   "table",
		PrimaryKeys: primaryKeys,
		FirstWhere:  GreaterThanEqualTo,
		SecondWhere: GreaterThan,
		Limit:       1,
		Columns: []schema.Column{
			{Name: "a", Type: schema.Int64},
			{Name: "b", Type: schema.Int64},
			{Name: "c", Type: schema.Int64},
			{Name: "e", Type: schema.Text},
			{Name: "f", Type: schema.Int64},
			{Name: "g", Type: schema.Money}, // money will be cast
		},
	})
	assert.Equal(t, `SELECT "a","b","c","e","f","g"::text FROM "schema"."table" WHERE row("a","b","c") >= row(1,2,3) AND NOT row("a","b","c") > row(4,5,6) ORDER BY "a","b","c" LIMIT 1`, query)
}
