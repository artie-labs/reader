package mysql

import (
	"database/sql"
	"fmt"
	"github.com/artie-labs/reader/config"
)

type Source struct {
	cfg config.MySQL
	db  *sql.DB
}

func Load(cfg config.MySQL) (*Source, bool, error) {
	db, err := sql.Open("mysql", cfg.ToDSN())
	if err != nil {
		return nil, false, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	return &Source{cfg: cfg, db: db}, cfg.StreamingSettings.Enabled, nil
}
