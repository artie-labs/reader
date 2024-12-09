package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/lib/transfer"
	"github.com/artie-labs/reader/sources/mysql/adapter"
	"github.com/artie-labs/reader/writers"
)

type Snapshot struct {
	cfg config.MySQL
	db  *sql.DB
}

func (s Snapshot) Close() error {
	return s.db.Close()
}

func (s *Snapshot) Run(ctx context.Context, writer writers.Writer) error {
	for _, tableCfg := range s.cfg.Tables {
		if err := s.snapshotTable(ctx, writer, *tableCfg); err != nil {
			return err
		}
	}
	return nil
}

func (s Snapshot) snapshotTable(ctx context.Context, writer writers.Writer, tableCfg config.MySQLTable) error {
	logger := slog.With(slog.String("table", tableCfg.Name), slog.String("database", s.cfg.Database))
	snapshotStartTime := time.Now()

	dbzAdapter, err := adapter.NewMySQLAdapter(s.db, s.cfg.Database, tableCfg)
	if err != nil {
		return fmt.Errorf("failed to create MySQL adapter: %w", err)
	}

	if err := writer.BeforeBackfill(ctx, dbzAdapter.TableName()); err != nil {
		return err
	}

	dbzTransformer, err := transformer.NewDebeziumTransformer(dbzAdapter)
	if err != nil {
		if errors.Is(err, rdbms.ErrNoPkValuesForEmptyTable) {
			cols, err := transfer.BuildTransferColumns(dbzAdapter)
			if err != nil {
				return fmt.Errorf("failed to build transfer columns: %w", err)
			}

			if err = writer.CreateTable(ctx, dbzAdapter.TableName(), cols); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}

			logger.Info("Table has been created, it does not contain any rows")
			return nil
		} else {
			return fmt.Errorf("failed to build Debezium transformer for table %q: %w", tableCfg.Name, err)
		}
	}

	logger.Info("Scanning table...", slog.Any("batchSize", tableCfg.GetBatchSize()))
	count, err := writer.Write(ctx, dbzTransformer)
	if err != nil {
		return fmt.Errorf("failed to snapshot table %q: %w", tableCfg.Name, err)
	}

	logger.Info("Finished snapshotting",
		slog.Int("scannedTotal", count),
		slog.Duration("totalDuration", time.Since(snapshotStartTime)),
	)
	return nil
}
