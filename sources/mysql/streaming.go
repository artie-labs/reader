package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources/mysql/streaming"
	"github.com/artie-labs/reader/writers"
)

type Streaming struct {
	iterator *streaming.Iterator
	db       *sql.DB
}

func buildStreamingConfig(db *sql.DB, cfg config.MySQL, sqlMode string) (Streaming, error) {
	iter, err := streaming.BuildStreamingIterator(db, cfg, sqlMode)
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
