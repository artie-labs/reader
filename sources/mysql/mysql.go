package mysql

import (
	"database/sql"
	"fmt"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources"
)

func Load(cfg config.MySQL) (sources.Source, bool, error) {
	db, err := sql.Open("mysql", cfg.ToDSN())
	if err != nil {
		return nil, false, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	// TODO: Instead of running describe table and parsing the data back out in tabular format, we should run get create table ddl and feed that through our DDL parser.
	if cfg.StreamingSettings.Enabled {
		stream, err := buildStreamingConfig(cfg)
		if err != nil {
			return nil, false, fmt.Errorf("failed to build streaming config: %w", err)
		}

		return &stream, true, nil
	}

	return &Snapshot{cfg: cfg, db: db}, false, nil
}
