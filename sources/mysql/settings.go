package mysql

import (
	"database/sql"
	"fmt"
)

type Settings struct {
	Version string
	SQLMode string
}

func retrieveSettings(db *sql.DB) (Settings, error) {
	version, err := retrieveVersion(db)
	if err != nil {
		return Settings{}, fmt.Errorf("failed to retrieve MySQL version: %w", err)
	}

	sqlMode, err := retrieveSessionSQLMode(db)
	if err != nil {
		return Settings{}, fmt.Errorf("failed to retrieve MySQL session sql_mode: %w", err)
	}

	return Settings{
		Version: version,
		SQLMode: sqlMode,
	}, nil
}

func retrieveVersion(db *sql.DB) (string, error) {
	var version string
	if err := db.QueryRow(`SELECT VERSION();`).Scan(&version); err != nil {
		return "", err
	}

	return version, nil
}

func retrieveSessionSQLMode(db *sql.DB) (string, error) {
	var sqlMode string
	if err := db.QueryRow(`SELECT @@SESSION.sql_mode;`).Scan(&sqlMode); err != nil {
		return "", err
	}

	return sqlMode, nil
}
