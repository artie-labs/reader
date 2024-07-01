package mongo

import (
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/persistedmap"
	"go.mongodb.org/mongo-driver/mongo"
)

type streaming struct {
	db  *mongo.Database
	cfg config.MongoDB

	offsets *persistedmap.PersistedMap
}

func newStreamingIterator(db *mongo.Database, cfg config.MongoDB, filePath string) *streaming {
	return &streaming{
		db:      db,
		cfg:     cfg,
		offsets: persistedmap.NewPersistedMap(filePath),
	}
}

func (s *streaming) HasNext() bool {
	// Streaming mode always has next
	return true
}

func (s *streaming) Next() ([]lib.RawMessage, error) {
	return nil, fmt.Errorf("not implemented")
}
