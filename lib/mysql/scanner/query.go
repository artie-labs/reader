package scanner

import (
	"fmt"
	"strings"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

func sqlPlaceholders(count int) []string {
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = "?"
	}
	return result
}

type buildScanTableQueryArgs struct {
	TableName           string
	PrimaryKeys         *primary_key.Keys
	Columns             []schema.Column
	InclusiveLowerBound bool
	Limit               uint
}

func buildScanTableQuery(args buildScanTableQueryArgs) (string, []any, error) {
	colNames := make([]string, len(args.Columns))
	for idx, col := range args.Columns {
		colNames[idx] = schema.QuoteIdentifier(col.Name)
	}

	var startingValues = make([]any, len(args.PrimaryKeys.KeyNames()))
	var endingValues = make([]any, len(startingValues))
	for i, pk := range args.PrimaryKeys.Keys() {
		startingValues[i] = pk.StartingValue
		endingValues[i] = pk.EndingValue
	}

	lowerBoundComparison := ">"
	if args.InclusiveLowerBound {
		lowerBoundComparison = ">="
	}

	// TODO: use slices.Concat when we upgrade to Go 1.22
	var parameters []any
	parameters = append(parameters, startingValues...)
	parameters = append(parameters, endingValues...)

	return fmt.Sprintf(`SELECT %s FROM %s WHERE (%s) %s (%s) AND (%s) <= (%s) ORDER BY %s LIMIT %d`,
		// SELECT
		strings.Join(colNames, ","),
		// FROM
		schema.QuoteIdentifier(args.TableName),
		// WHERE (pk) > (123)
		strings.Join(schema.QuotedIdentifiers(args.PrimaryKeys.KeyNames()), ","), lowerBoundComparison, strings.Join(sqlPlaceholders(len(startingValues)), ","),
		// AND NOT (pk) <= (123)
		strings.Join(schema.QuotedIdentifiers(args.PrimaryKeys.KeyNames()), ","), strings.Join(sqlPlaceholders(len(endingValues)), ","),
		// ORDER BY
		strings.Join(schema.QuotedIdentifiers(args.PrimaryKeys.KeyNames()), ","),
		// LIMIT
		args.Limit,
	), parameters, nil
}
