package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources/mysql/streaming"
	"github.com/artie-labs/reader/writers"
)

type Streaming struct {
	iterator *streaming.Iterator
	db       *sql.DB
}

func buildStreamingConfig(ctx context.Context, db *sql.DB, cfg config.MySQL, sqlMode []string, gtidEnabled bool) (Streaming, error) {
	// Validate to ensure that we can use streaming.
	if err := ValidateMySQL(ctx, db, true); err != nil {
		return Streaming{}, fmt.Errorf("failed validation: %w", err)
	}

	iter, err := streaming.BuildStreamingIterator(db, cfg, sqlMode, gtidEnabled)
	if err != nil {
		return Streaming{}, err
	}

	return Streaming{
		db:       db,
		iterator: &iter,
	}, nil
}

func (s Streaming) Close() error {
	if err := s.iterator.Close(); err != nil {
		return fmt.Errorf("failed to close iterator: %w", err)
	}

	return s.db.Close()
}

func (s Streaming) Run(ctx context.Context, writer writers.Writer) error {
	_, err := writer.Write(ctx, s.iterator)
	return err
}

func hasGTIDEnabled(ctx context.Context, db *sql.DB) (bool, error) {
	requiredVariables := []string{"gtid_mode", "enforce_gtid_consistency"}
	for _, requiredVariable := range requiredVariables {
		value, err := fetchVariable(ctx, db, requiredVariable)
		if err != nil {
			return false, err
		}

		if strings.ToUpper(value) != "ON" {
			return false, nil
		}
	}

	return true, nil
}
