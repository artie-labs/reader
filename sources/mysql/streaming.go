package mysql

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/sources/mysql/streaming"
	"github.com/artie-labs/reader/writers"
)

type Streaming struct {
	iterator iterator.StreamingIterator[[]lib.RawMessage]
}

func buildStreamingConfig(cfg config.MySQL) (Streaming, error) {
	iter, err := streaming.BuildStreamingIterator(cfg)
	if err != nil {
		return Streaming{}, err
	}

	return Streaming{iterator: iter}, nil
}

func (s Streaming) Run(ctx context.Context, writer writers.Writer) error {
	_, err := writer.Write(ctx, s.iterator)
	return err
}

func (s Streaming) Close() error {
	return nil
}
