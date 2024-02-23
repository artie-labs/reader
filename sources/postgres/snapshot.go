package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/sources/postgres/adapter"
)

const defaultErrorRetries = 10

type Source struct {
	cfg config.PostgreSQL
	db  *sql.DB
}

func Load(cfg config.PostgreSQL) (*Source, error) {
	db, err := sql.Open("pgx", cfg.ToDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	return &Source{
		cfg: cfg,
		db:  db,
	}, nil
}

func (s *Source) Close() error {
	return s.db.Close()
}

func (s *Source) Run(ctx context.Context, writer kafkalib.BatchWriter) error {
	for _, tableCfg := range s.cfg.Tables {
		snapshotStartTime := time.Now()

		slog.Info("Loading configuration for table", slog.String("table", tableCfg.Name), slog.String("schema", tableCfg.Schema))
		table := postgres.NewTable(tableCfg.Schema, tableCfg.Name)
		if err := table.PopulateColumns(s.db); err != nil {
			if rdbms.IsNoRowsErr(err) {
				slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name), slog.String("schema", table.Schema))
				continue
			} else {
				return fmt.Errorf("failed to load configuration for table %s.%s: %w", table.Schema, table.Name, err)
			}
		}

		scanner, err := table.NewScanner(
			s.db,
			tableCfg.GetBatchSize(),
			tableCfg.GetOptionalPrimaryKeyValStart(),
			tableCfg.GetOptionalPrimaryKeyValEnd(),
			defaultErrorRetries,
		)
		if err != nil {
			return fmt.Errorf("failed to build scanner for table %s: %w", table.Name, err)
		}
		dbzTransformer := debezium.NewDebeziumTransformer(adapter.NewPostgresAdapter(*table), &scanner)
		count, err := writer.WriteIterator(ctx, dbzTransformer)
		if err != nil {
			return fmt.Errorf("failed to snapshot for table %s: %w", table.Name, err)
		}

		slog.Info("Finished snapshotting",
			slog.Int("scannedTotal", count),
			slog.Duration("totalDuration", time.Since(snapshotStartTime)),
		)
	}

	return nil
}
