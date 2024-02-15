package queries

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestQuotedIdentifiers(t *testing.T) {
	assert.Equal(t, []string{`"a"`, `"bb""bb"`, `"c"`}, quotedIdentifiers([]string{"a", `bb"bb`, "c"}))
}

func TestScanTableQuery(t *testing.T) {
	query := ScanTableQuery(ScanTableQueryArgs{
		Schema:       "schema",
		TableName:    "table",
		PrimaryKeys:  []string{"a", "b", "c"},
		FirstWhere:   GreaterThanEqualTo,
		StartingKeys: []string{"1", "2", "3"},
		SecondWhere:  GreaterThan,
		EndingKeys:   []string{"4", "5", "6"},
		OrderBy:      []string{"order"},
		Limit:        1,
		ColumnsToScan: []schema.Column{
			{Name: "e", Type: schema.Int16},
			{Name: "f", Type: schema.Int16},
			{Name: "g", Type: schema.Bit},
		},
		ColumnCast: func(col schema.Column) string {
			if col.Type == schema.Bit {
				return col.Name + "::text"
			} else {
				return col.Name
			}
		},
	})
	assert.Equal(t, `SELECT e,f,g::text FROM "schema"."table" WHERE row("a","b","c") >= row(1,2,3) AND NOT row("a","b","c") > row(4,5,6) ORDER BY "order" LIMIT 1`, query)
}
