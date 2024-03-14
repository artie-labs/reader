package postgres

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func QuotedIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for idx := range ids {
		quoted[idx] = pgx.Identifier{ids[idx]}.Sanitize()
	}
	return quoted
}

func QueryPlaceholders(offset, count int) []string {
	result := make([]string, count)
	for i := range count {
		result[i] = fmt.Sprintf("$%d", 1+offset+i)
	}
	return result
}
