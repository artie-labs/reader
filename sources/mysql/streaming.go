package mysql

import (
	"context"
	"database/sql"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources/mysql/streaming"
	"github.com/artie-labs/reader/writers"
)

type Streaming struct {
	iterator *streaming.Iterator
}

func buildStreamingConfig(db *sql.DB, cfg config.MySQL) (Streaming, error) {
	iter, err := streaming.BuildStreamingIterator(db, cfg)
	if err != nil {
		return Streaming{}, err
	}

	return Streaming{
		iterator: &iter,
	}, nil
}

func (s Streaming) Close() error {
	return s.iterator.Close()
}

func (s Streaming) Run(ctx context.Context, writer writers.Writer) error {
	_, err := writer.Write(ctx, s.iterator)
	return err
}
