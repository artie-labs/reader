package scanner

import (
	"fmt"
	"slices"
	"strings"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

func queryPlaceholders(count int) []string {
	result := make([]string, count)
	for i := range count {
		result[i] = "?"
	}
	return result
}

func buildScanTableQuery(tableName string,
	primaryKeys []primary_key.Key,
	columns []schema.Column,
	inclusiveLowerBound bool,
	limit uint,
) (string, []any, error) {
	colNames := make([]string, len(columns))
	for idx, col := range columns {
		colNames[idx] = schema.QuoteIdentifier(col.Name)
	}

	var startingValues = make([]any, len(primaryKeys))
	var endingValues = make([]any, len(startingValues))
	for i, pk := range primaryKeys {
		startingValues[i] = pk.StartingValue
		endingValues[i] = pk.EndingValue
	}

	quotedKeyNames := make([]string, len(primaryKeys))
	for i, key := range primaryKeys {
		quotedKeyNames[i] = schema.QuoteIdentifier(key.Name)
	}

	lowerBoundComparison := ">"
	if inclusiveLowerBound {
		lowerBoundComparison = ">="
	}

	return fmt.Sprintf(`SELECT %s FROM %s WHERE (%s) %s (%s) AND (%s) <= (%s) ORDER BY %s LIMIT %d`,
		// SELECT
		strings.Join(colNames, ","),
		// FROM
		schema.QuoteIdentifier(tableName),
		// WHERE (pk) > (123)
		strings.Join(quotedKeyNames, ","), lowerBoundComparison, strings.Join(queryPlaceholders(len(startingValues)), ","),
		// AND NOT (pk) <= (123)
		strings.Join(quotedKeyNames, ","), strings.Join(queryPlaceholders(len(endingValues)), ","),
		// ORDER BY
		strings.Join(quotedKeyNames, ","),
		// LIMIT
		limit,
	), slices.Concat(startingValues, endingValues), nil
}
