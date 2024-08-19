package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
	mongoLib "github.com/artie-labs/reader/lib/mongo"
)

type snapshotIterator struct {
	db         *mongo.Database
	cfg        config.MongoDB
	collection config.Collection

	// mutable
	cursor *mongo.Cursor
	done   bool
}

func NewSnapshotIterator(db *mongo.Database, collection config.Collection, cfg config.MongoDB) iterator.Iterator[[]lib.RawMessage] {
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
		// Filter
		filter := bson.D{}
		if s.collection.StartObjectID != "" {
			key, err := primitive.ObjectIDFromHex(s.collection.StartObjectID)
			if err != nil {
				return nil, fmt.Errorf("failed to parse start object id %q: %w", s.collection.StartObjectID, err)
			}

			filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$gte", Value: key}}})
		}

		if s.collection.EndObjectID != "" {
			key, err := primitive.ObjectIDFromHex(s.collection.EndObjectID)
			if err != nil {
				return nil, fmt.Errorf("failed to parse end object id %q: %w", s.collection.EndObjectID, err)
			}

			filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lte", Value: key}}})
		}

		// Find options
		findOptions := options.Find()
		findOptions.SetBatchSize(s.collection.GetBatchSize())
		cursor, err := s.db.Collection(s.collection.Name).Find(ctx, filter, findOptions)
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

		mgoMsg, err := mongoLib.ParseMessage(result, nil, "r")
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
