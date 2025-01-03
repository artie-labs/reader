package mssql

import (
	"database/sql"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources"
)

func Load(cfg config.MSSQL) (sources.Source, bool, error) {
	fmt.Println(" cfg.ToDSN()", cfg.ToDSN())
	db, err := sql.Open("mssql", cfg.ToDSN())
	if err != nil {
		return nil, false, fmt.Errorf("failed to connect to MSSQL: %w", err)
	}

	if cfg.Streaming {
		return &Streamer{cfg: cfg, db: db}, true, nil
	}

	return &Snapshot{cfg: cfg, db: db}, false, nil
}
