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
	requiredVariableToValueMap := make(map[string]string)
	if validateStreaming {
		requiredVariableToValueMap["binlog_format"] = "ROW"
	}

	if validateGTID {
		requiredVariableToValueMap["gtid_mode"] = "ON"
		requiredVariableToValueMap["enforce_gtid_consistency"] = "ON"
	}

	for requiredVariable, requiredValue := range requiredVariableToValueMap {
		value, err := fetchVariable(ctx, db, requiredVariable)
		if err != nil {
			return err
		}

		if strings.ToUpper(value) != requiredValue {
			return fmt.Errorf("%s must be set to %q, current value is %q", requiredVariable, requiredValue, value)
		}
	}

	return nil
}
