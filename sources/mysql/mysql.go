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

	settings, err := retrieveSettings(db)
	if err != nil {
		return nil, false, fmt.Errorf("failed to retrieve MySQL settings: %w", err)
	}

	slog.Info("Loading MySQL connector",
		slog.String("version", settings.Version),
		slog.Any("sqlMode", settings.SQLMode),
	)
	if cfg.StreamingSettings.Enabled {
		stream, err := buildStreamingConfig(db, cfg, settings.SQLMode)
		if err != nil {
			return nil, false, fmt.Errorf("failed to build streaming config: %w", err)
		}

		return stream, true, nil
	}

	return &Snapshot{cfg: cfg, db: db}, false, nil
}
