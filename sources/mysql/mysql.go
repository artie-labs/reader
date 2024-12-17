package mysql

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources"
)

func Load(cfg config.MySQL) (sources.Source, bool, error) {
	db, err := sql.Open("mysql", cfg.ToDSN())
	if err != nil {
		return nil, false, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	version, err := retrieveVersion(db)
	if err != nil {
		return nil, false, fmt.Errorf("failed to retrieve MySQL version: %w", err)
	}

	sqlMode, err := retrieveSessionSQLMode(db)
	if err != nil {
		return nil, false, fmt.Errorf("failed to retrieve MySQL session sql_mode: %w", err)
	}

	slog.Info("Loading MySQL connector",
		slog.String("version", version),
		slog.String("sqlMode", sqlMode),
	)
	if cfg.StreamingSettings.Enabled {
		stream, err := buildStreamingConfig(db, cfg, sqlMode)
		if err != nil {
			return nil, false, fmt.Errorf("failed to build streaming config: %w", err)
		}

		return stream, true, nil
	}

	return &Snapshot{cfg: cfg, db: db}, false, nil
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
