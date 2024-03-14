package postgres

import (
	"strings"
)

// QuoteLiteral quotes a 'literal' (e.g. a parameter, often used to pass literal
// to DDL and other statements that do not accept parameters) to be used as part
// of an SQL statement.  For example:
//
//	exp_date := pq.QuoteLiteral("2023-01-05 15:00:00Z")
//	err := db.Exec(fmt.Sprintf("CREATE ROLE my_user VALID UNTIL %s", exp_date))
//
// Any single quotes in name will be escaped. Any backslashes (i.e. "\") will be
// replaced by two backslashes (i.e. "\\") and the C-style escape identifier
// that PostgreSQL provides ('E') will be prepended to the string.
// Copied from: https://github.com/lib/pq/blob/3d613208bca2e74f2a20e04126ed30bcb5c4cc27/conn.go#L1687
func QuoteLiteral(literal string) string {
	// This follows the PostgreSQL internal algorithm for handling quoted literals
	// from libpq, which can be found in the "PQEscapeStringInternal" function,
	// which is found in the libpq/fe-exec.c source file:
	// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/interfaces/libpq/fe-exec.c
	//
	// substitute any single-quotes (') with two single-quotes ('')
	literal = strings.Replace(literal, `'`, `''`, -1)
	// determine if the string has any backslashes (\) in it.
	// if it does, replace any backslashes (\) with two backslashes (\\)
	// then, we need to wrap the entire string with a PostgreSQL
	// C-style escape. Per how "PQEscapeStringInternal" handles this case, we
	// also add a space before the "E"
	if strings.Contains(literal, `\`) {
		literal = strings.Replace(literal, `\`, `\\`, -1)
		literal = ` E'` + literal + `'`
	} else {
		// otherwise, we can just wrap the literal with a pair of single quotes
		literal = `'` + literal + `'`
	}
	return literal
}
