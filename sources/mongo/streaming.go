package mongo

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/persistedmap"
	"go.mongodb.org/mongo-driver/mongo"
)

type streaming struct {
	db                    *mongo.Database
	cfg                   config.MongoDB
	changeStream          *mongo.ChangeStream
	ctx                   context.Context
	collectionsToWatchMap map[string]bool
	offsets               *persistedmap.PersistedMap
}

func newStreamingIterator(ctx context.Context, db *mongo.Database, cfg config.MongoDB, filePath string) (*streaming, error) {
	collectionsToWatchMap := make(map[string]bool)
	for _, collection := range cfg.Collections {
		collectionsToWatchMap[collection.Name] = true
	}

	cs, err := db.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		return nil, fmt.Errorf("failed to start change stream: %w", err)
	}

	return &streaming{
		db:                    db,
		cfg:                   cfg,
		changeStream:          cs,
		ctx:                   ctx,
		collectionsToWatchMap: collectionsToWatchMap,
		offsets:               persistedmap.NewPersistedMap(filePath),
	}, nil
}

func (s *streaming) HasNext() bool {
	// Streaming mode always has next
	return s.changeStream.Next(s.ctx)
}

func (s *streaming) Next() ([]lib.RawMessage, error) {

	return nil, fmt.Errorf("not implemented")
}
