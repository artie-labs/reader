package mongo

import (
	"context"
	"fmt"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type snapshotIterator struct {
	db         *mongo.Database
	cfg        config.MongoDB
	collection config.Collection

	// mutable
	cursor *mongo.Cursor
	done   bool
}

func newSnapshotIterator(db *mongo.Database, collection config.Collection, cfg config.MongoDB) *snapshotIterator {
	return &snapshotIterator{
		db:         db,
		cfg:        cfg,
		collection: collection,
	}
}

func (s *snapshotIterator) HasNext() bool {
	return !s.done
}

func (s *snapshotIterator) Next() ([]lib.RawMessage, error) {
	if !s.HasNext() {
		return nil, fmt.Errorf("no more rows to scan")
	}

	ctx := context.Background()
	if s.cursor == nil {
		findOptions := options.Find()
		findOptions.SetBatchSize(s.collection.GetBatchSize())
		cursor, err := s.db.Collection(s.collection.Name).Find(ctx, bson.D{}, findOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to find documents: %w", err)
		}

		s.cursor = cursor
	}

	var rawMsgs []lib.RawMessage
	for s.collection.GetBatchSize() > int32(len(rawMsgs)) && s.cursor.Next(ctx) {
		var result bson.M
		if err := s.cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode document: %w", err)
		}

		mgoMsg, err := ParseMessage(result, "r")
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}

		rawMsg, err := mgoMsg.ToRawMessage(s.collection, s.cfg.Database)
		if err != nil {
			return nil, fmt.Errorf("failed to create raw message: %w", err)
		}

		rawMsgs = append(rawMsgs, rawMsg)
	}

	if err := s.cursor.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over documents: %w", err)
	}

	// If the number of fetched documents is less than the batch size, we are done
	if s.collection.GetBatchSize() > int32(len(rawMsgs)) {
		s.done = true
	}

	return rawMsgs, nil
}
