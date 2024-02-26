package scanner

import (
	"fmt"
	"strings"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

func sqlPlaceholders(count int) []string {
	result := make([]string, count)
	for i := range count {
		result[i] = "?"
	}
	return result
}

type buildScanTableQueryArgs struct {
	TableName           string
	PrimaryKeys         []primary_key.Key
	Columns             []schema.Column
	InclusiveLowerBound bool
	Limit               uint
}

func buildScanTableQuery(args buildScanTableQueryArgs) (string, []any, error) {
	colNames := make([]string, len(args.Columns))
	for idx, col := range args.Columns {
		colNames[idx] = schema.QuoteIdentifier(col.Name)
	}

	var startingValues = make([]any, len(args.PrimaryKeys))
	var endingValues = make([]any, len(startingValues))
	for i, pk := range args.PrimaryKeys {
		startingValues[i] = pk.StartingValue
		endingValues[i] = pk.EndingValue
	}

	quotedKeyNames := make([]string, len(args.PrimaryKeys))
	for i, key := range args.PrimaryKeys {
		quotedKeyNames[i] = schema.QuoteIdentifier(key.Name)
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
		strings.Join(quotedKeyNames, ","), lowerBoundComparison, strings.Join(sqlPlaceholders(len(startingValues)), ","),
		// AND NOT (pk) <= (123)
		strings.Join(quotedKeyNames, ","), strings.Join(sqlPlaceholders(len(endingValues)), ","),
		// ORDER BY
		strings.Join(quotedKeyNames, ","),
		// LIMIT
		args.Limit,
	), parameters, nil
}
