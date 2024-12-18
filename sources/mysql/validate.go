package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func fetchVariable(ctx context.Context, db *sql.DB, name string) (string, error) {
	row := db.QueryRowContext(ctx, "SHOW VARIABLES WHERE variable_name = ?", name)
	if row.Err() != nil {
		return "", fmt.Errorf("failed to query for %q variable: %w", name, row.Err())
	}

	var variableName string
	var value string
	if err := row.Scan(&variableName, &value); err != nil {
		return "", fmt.Errorf("failed to scan row: %w", err)
	} else if variableName != name {
		return "", fmt.Errorf("the variable %q was returned instead of %q", variableName, name)
	}

	return value, nil
}

func ValidateMySQL(ctx context.Context, db *sql.DB, validateStreaming bool) error {
	if validateStreaming {
		value, err := fetchVariable(ctx, db, "binlog_format")
		if err != nil {
			return err
		}

		if strings.ToUpper(value) != "ROW" {
			return fmt.Errorf("'binlog_format' must be set to 'ROW', current value is '%s'", value)
		}
	}

	return nil
}
