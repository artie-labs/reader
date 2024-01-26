package queries

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

type DescribeTableArgs struct {
	Name   string
	Schema string
}

const describeTableQuery = `
SELECT column_name, data_type, numeric_precision, numeric_scale, udt_name
FROM information_schema.columns
WHERE table_name = $1 AND table_schema = $2`

func DescribeTableQuery(args DescribeTableArgs) (string, []any) {
	return strings.TrimSpace(describeTableQuery), []any{args.Name, args.Schema}
}

func quotedIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for idx := range ids {
		quoted[idx] = pq.QuoteIdentifier(ids[idx])
	}
	return quoted
}

type SelectTableQueryArgs struct {
	Keys       []string
	Schema     string
	TableName  string
	OrderBy    []string
	Descending bool
}

func SelectTableQuery(args SelectTableQueryArgs) string {
	orderByFragment := strings.Join(quotedIdentifiers(args.OrderBy), ",")
	if args.Descending {
		orderByFragment += " DESC"
	}

	// TODO: Make sure keys are being escaped properly
	return fmt.Sprintf(`SELECT %s FROM %s.%s ORDER BY %s LIMIT 1`,
		strings.Join(args.Keys, ","), pq.QuoteIdentifier(args.Schema), pq.QuoteIdentifier(args.TableName), orderByFragment)
}

type RetrievePrimaryKeysArgs struct {
	Schema    string
	TableName string
}

const primaryKeysQuery = `
SELECT a.attname::text as id
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE  i.indrelid = $1::regclass
AND    i.indisprimary;`

func RetrievePrimaryKeys(args RetrievePrimaryKeysArgs) (string, []any) {
	// This is a fork of: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
	return strings.TrimSpace(primaryKeysQuery), []any{fmt.Sprintf("%s.%s", pq.QuoteIdentifier(args.Schema), pq.QuoteIdentifier(args.TableName))}
}

type Comparison string

const (
	GreaterThan        Comparison = ">"
	GreaterThanEqualTo Comparison = ">="
)

func (c Comparison) SQLString() string {
	if (c == GreaterThan) || (c == GreaterThanEqualTo) {
		return string(c)
	}
	panic(fmt.Sprintf("invalid comparison: '%v'", c))
}

type ScanTableQueryArgs struct {
	Schema        string
	TableName     string
	PrimaryKeys   []string
	ColumnsToScan []string

	// First where clause
	FirstWhere   Comparison
	StartingKeys []string
	// Second where clause
	SecondWhere Comparison
	EndingKeys  []string

	OrderBy []string
	Limit   uint
}

func ScanTableQuery(args ScanTableQueryArgs) string {
	return fmt.Sprintf(`SELECT %s FROM %s.%s WHERE row(%s) %s row(%s) AND NOT row(%s) %s row(%s) ORDER BY %s LIMIT %d`,
		strings.Join(args.ColumnsToScan, ","),
		pq.QuoteIdentifier(args.Schema), pq.QuoteIdentifier(args.TableName),
		// WHERE row(pk) > row(123)
		strings.Join(quotedIdentifiers(args.PrimaryKeys), ","), args.FirstWhere.SQLString(), strings.Join(args.StartingKeys, ","),
		// AND NOT row(pk) < row(123)
		strings.Join(quotedIdentifiers(args.PrimaryKeys), ","), args.SecondWhere.SQLString(), strings.Join(args.EndingKeys, ","),
		strings.Join(quotedIdentifiers(args.OrderBy), ","),
		args.Limit,
	)
}
