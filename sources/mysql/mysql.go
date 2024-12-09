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

	if cfg.StreamingSettings.Enabled {
		stream, err := buildStreamingConfig(db, cfg)
		if err != nil {
			return nil, false, fmt.Errorf("failed to build streaming config: %w", err)
		}

		return stream, true, nil
	}

	return &Snapshot{cfg: cfg, db: db}, false, nil
}
