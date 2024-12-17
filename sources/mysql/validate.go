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

func ValidateMySQL(ctx context.Context, db *sql.DB, validateStreaming bool, validateGTID bool) error {
	if validateStreaming {
		// Make sure it's row format
		binlogFormat, err := fetchVariable(ctx, db, "binlog_format")
		if err != nil {
			return err
		}

		if strings.ToUpper(binlogFormat) != "ROW" {
			return fmt.Errorf("binlog_format must be set to 'ROW', current value is %q", binlogFormat)
		}
	}

	if validateGTID {
		expectedVariableToValueMap := map[string]string{
			"gtid_mode":                "ON",
			"enforce_gtid_consistency": "ON",
		}

		for expectedVariable, expectedValue := range expectedVariableToValueMap {
			value, err := fetchVariable(ctx, db, expectedVariable)
			if err != nil {
				return err
			}

			if strings.ToUpper(value) != expectedValue {
				return fmt.Errorf("%s must be set to %q, current value is %q", expectedVariable, expectedValue, value)
			}
		}
	}

	return nil
}
