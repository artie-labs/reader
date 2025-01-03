package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources"
)

func Load(ctx context.Context, cfg config.MySQL) (sources.Source, bool, error) {
	db, err := sql.Open("mysql", cfg.ToDSN())
	if err != nil {
		return nil, false, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	settings, err := retrieveSettings(ctx, db)
	if err != nil {
		return nil, false, fmt.Errorf("failed to retrieve MySQL settings: %w", err)
	}

	slog.Info("Loading MySQL connector",
		slog.String("version", settings.Version),
		slog.Any("sqlMode", settings.SQLMode),
		slog.Bool("gtidEnabled", settings.GTIDEnabled),
	)

	if cfg.StreamingSettings.Enabled {
		stream, err := buildStreamingConfig(ctx, db, cfg, settings.SQLMode, settings.GTIDEnabled)
		if err != nil {
			return nil, false, fmt.Errorf("failed to build streaming config: %w", err)
		}

		return stream, true, nil
	}

	return &Snapshot{cfg: cfg, db: db}, false, nil
}
