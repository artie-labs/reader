package mssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/lib/transfer"
	"github.com/artie-labs/reader/sources/mssql/adapter"
	"github.com/artie-labs/reader/writers"
)

type Source struct {
	cfg config.MSSQL
	db  *sql.DB
}

func Load(cfg config.MSSQL) (*Source, error) {
	db, err := sql.Open("mssql", cfg.ToDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MSSQL: %w", err)
	}

	return &Source{
		cfg: cfg,
		db:  db,
	}, nil
}

func (s *Source) Close() error {
	return s.db.Close()
}

func (s *Source) Run(ctx context.Context, writer writers.Writer) error {
	for _, tableCfg := range s.cfg.Tables {
		logger := slog.With(slog.String("schema", tableCfg.Schema), slog.String("table", tableCfg.Name))
		snapshotStartTime := time.Now()

		dbzAdapter, err := adapter.NewMSSQLAdapter(s.db, s.cfg.Database, *tableCfg)
		if err != nil {
			return fmt.Errorf("failed to create MSSQL adapter: %w", err)
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
				continue
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
	}

	return nil
}
