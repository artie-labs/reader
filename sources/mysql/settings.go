package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type Settings struct {
	Version     string
	SQLMode     []string
	GTIDEnabled bool
}

func retrieveSettings(ctx context.Context, db *sql.DB) (Settings, error) {
	version, err := retrieveVersion(db)
	if err != nil {
		return Settings{}, fmt.Errorf("failed to retrieve MySQL version: %w", err)
	}

	sqlMode, err := retrieveSessionSQLMode(db)
	if err != nil {
		return Settings{}, fmt.Errorf("failed to retrieve MySQL session sql_mode: %w", err)
	}

	gtidEnabled, err := hasGTIDEnabled(ctx, db)
	if err != nil {
		return Settings{}, fmt.Errorf("failed to check if GTID is enabled: %w", err)
	}

	return Settings{
		Version:     version,
		SQLMode:     sqlMode,
		GTIDEnabled: gtidEnabled,
	}, nil
}

func retrieveVersion(db *sql.DB) (string, error) {
	var version string
	if err := db.QueryRow(`SELECT VERSION();`).Scan(&version); err != nil {
		return "", err
	}

	return version, nil
}

func retrieveSessionSQLMode(db *sql.DB) ([]string, error) {
	var sqlMode string
	if err := db.QueryRow(`SELECT @@SESSION.sql_mode;`).Scan(&sqlMode); err != nil {
		return nil, err
	}

	return strings.Split(sqlMode, ","), nil
}

func hasGTIDEnabled(ctx context.Context, db *sql.DB) (bool, error) {
	requiredVariables := []string{"gtid_mode", "enforce_gtid_consistency"}
	for _, requiredVariable := range requiredVariables {
		value, err := fetchVariable(ctx, db, requiredVariable)
		if err != nil {
			return false, err
		}

		if strings.ToUpper(value) != "ON" {
			return false, nil
		}
	}

	return true, nil
}
