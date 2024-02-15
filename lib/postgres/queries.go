package postgres

import (
	"github.com/jackc/pgx/v5"
)

func QuotedIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for idx := range ids {
		quoted[idx] = pgx.Identifier{ids[idx]}.Sanitize()
	}
	return quoted
}
