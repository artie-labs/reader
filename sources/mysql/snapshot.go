package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/sources/mysql/adapter"
)

const defaultErrorRetries = 10

type Source struct {
	cfg config.MySQL
	db  *sql.DB
}

func Load(cfg config.MySQL) (*Source, error) {
	db, err := sql.Open("mysql", cfg.ToDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	return &Source{
		cfg: cfg,
		db:  db,
	}, nil
}

func (s Source) Close() error {
	return s.db.Close()
}

func (s *Source) Run(ctx context.Context, writer kafkalib.BatchWriter) error {
	for _, tableCfg := range s.cfg.Tables {
		if err := s.snapshotTable(ctx, writer, *tableCfg); err != nil {
			return err
		}
	}
	return nil
}

func (s Source) snapshotTable(ctx context.Context, writer kafkalib.BatchWriter, tableCfg config.MySQLTable) error {
	snapshotStartTime := time.Now()

	slog.Info("Loading configuration for table", slog.String("table", tableCfg.Name))
	table := mysql.NewTable(tableCfg)
	if err := table.PopulateColumns(s.db); err != nil {
		if rdbms.IsNoRowsErr(err) {
			slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name))
			return nil
		} else {
			return fmt.Errorf("failed to load configuration for table %s: %w", table.Name, err)
		}
	}

	if err := table.PrimaryKeys.LoadValues(tableCfg.GetOptionalPrimaryKeyValStart(), tableCfg.GetOptionalPrimaryKeyValEnd()); err != nil {
		return fmt.Errorf("failed to override primary key values: %w", err)
	}

	slog.Info("Scanning table",
		slog.String("table", table.Name),
		slog.Any("primaryKeyColumns", table.PrimaryKeys.Keys()),
		slog.Any("batchSize", tableCfg.BatchSize),
	)

	scanner := table.NewScanner(s.db, tableCfg.GetBatchSize(), defaultErrorRetries)
	dbzTransformer := debezium.NewDebeziumTransformer(adapter.NewMySQLAdapter(*table), &scanner)
	count, err := writer.WriteIterator(ctx, dbzTransformer)
	if err != nil {
		return fmt.Errorf("failed to snapshot for table %s: %w", table.Name, err)
	}

	slog.Info("Finished snapshotting",
		slog.String("table", tableCfg.Name),
		slog.Int("scannedTotal", count),
		slog.Duration("totalDuration", time.Since(snapshotStartTime)),
	)

	return nil
}
