package queries

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDescribeTableQuery(t *testing.T) {
	{
		query, args := DescribeTableQuery(DescribeTableArgs{
			Name:   "name",
			Schema: "schema",
		})
		assert.Equal(t, "SELECT column_name, data_type, numeric_precision, numeric_scale, udt_name\nFROM information_schema.columns\nWHERE table_name = $1 AND table_schema = $2", query)
		assert.Equal(t, []any{"name", "schema"}, args)
	}
	// test quotes in table name or schema are left alone
	{
		_, args := DescribeTableQuery(DescribeTableArgs{
			Name:   `na"me`,
			Schema: `s'ch"em'a`,
		})
		assert.Equal(t, []any{`na"me`, `s'ch"em'a`}, args)
	}
}

func TestQuotedIdentifiers(t *testing.T) {
	assert.Equal(t, []string{`"a"`, `"bb""bb"`, `"c"`}, quotedIdentifiers([]string{"a", `bb"bb`, "c"}))
}

func TestSelectTableQuery(t *testing.T) {
	{
		query := SelectTableQuery(SelectTableQueryArgs{
			Keys:      []string{"a", "b", "c"},
			Schema:    "schema",
			TableName: "table",
			OrderBy:   []string{"e", "f", "g"},
		})
		assert.Equal(t, `SELECT a,b,c FROM "schema"."table" ORDER BY "e","f","g" LIMIT 1`, query)
	}
	// Descending
	{
		query := SelectTableQuery(SelectTableQueryArgs{
			Keys:       []string{"a", "b", "c"},
			Schema:     "schema",
			TableName:  "table",
			OrderBy:    []string{"e", "f", "g"},
			Descending: true,
		})
		assert.Equal(t, `SELECT a,b,c FROM "schema"."table" ORDER BY "e","f","g" DESC LIMIT 1`, query)
	}
}

func TestRetrievePrimaryKeys(t *testing.T) {
	{
		query, args := RetrievePrimaryKeys(RetrievePrimaryKeysArgs{
			Schema:    "schema",
			TableName: "table",
		})
		assert.Equal(t, "SELECT a.attname::text as id\nFROM   pg_index i\nJOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)\nWHERE  i.indrelid = $1::regclass\nAND    i.indisprimary;", query)
		assert.Equal(t, []any{`"schema"."table"`}, args)
	}
	{
		_, args := RetrievePrimaryKeys(RetrievePrimaryKeysArgs{
			Schema:    "schEMA",
			TableName: "__FOO",
		})

		assert.Equal(t, []any{`"schEMA"."__FOO"`}, args)
	}

}

func TestScanTableQuery(t *testing.T) {
	query := ScanTableQuery(ScanTableQueryArgs{
		Schema:        "schema",
		TableName:     "table",
		PrimaryKeys:   []string{"a", "b", "c"},
		FirstWhere:    GreaterThanEqualTo,
		StartingKeys:  []string{"1", "2", "3"},
		SecondWhere:   GreaterThan,
		EndingKeys:    []string{"4", "5", "6"},
		OrderBy:       []string{"order"},
		Limit:         1,
		ColumnsToScan: []string{"e", "f", "g"},
	})
	assert.Equal(t, `SELECT e,f,g FROM "schema"."table" WHERE row("a","b","c") >= row(1,2,3) AND NOT row("a","b","c") > row(4,5,6) ORDER BY "order" LIMIT 1`, query)
}
