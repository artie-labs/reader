package mongo

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/persistedmap"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type streaming struct {
	db                    *mongo.Database
	cfg                   config.MongoDB
	changeStream          *mongo.ChangeStream
	ctx                   context.Context
	collectionsToWatchMap map[string]config.Collection
	offsets               *persistedmap.PersistedMap
	batchSize             int32
}

func newStreamingIterator(ctx context.Context, db *mongo.Database, cfg config.MongoDB, filePath string) (*streaming, error) {
	collectionsToWatchMap := make(map[string]config.Collection)
	for _, collection := range cfg.Collections {
		collectionsToWatchMap[collection.Name] = collection
	}

	// Set pipeline to only show me insert, update and delete operations
	// Full list can be found here: https://www.mongodb.com/docs/manual/reference/change-events/
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{
			{"operationType", bson.D{
				{"$in", bson.A{"insert", "update", "delete"}},
			}},
		}}},
	}

	// TODO: Full document if available.
	cs, err := db.Watch(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to start change stream: %w", err)
	}

	return &streaming{
		// TODO: Consider making this configurable
		batchSize:             constants.DefaultBatchSize,
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
	var rawMsgs []lib.RawMessage

	// Check for new events
	if s.batchSize > int32(len(rawMsgs)) && s.changeStream.Next(s.ctx) {
		var changeEvent bson.M
		if err := s.changeStream.Decode(&changeEvent); err != nil {
			return nil, fmt.Errorf("failed to decode change event: %v", err)
		}

		ns, isOk := changeEvent["ns"]
		if !isOk {
			return nil, fmt.Errorf("failed to get namespace from change event: %v", changeEvent)
		}

		nsBsonM, isOk := ns.(bson.M)
		if !isOk {
			return nil, fmt.Errorf("expected ns to be bson.M, got: %T", ns)
		}

		coll, isOk := nsBsonM["coll"]
		if !isOk {
			return nil, fmt.Errorf("failed to get collection from change event: %v", changeEvent)
		}

		collString, isOk := coll.(string)
		if !isOk {
			return nil, fmt.Errorf("expected collection to be string, got: %T", coll)
		}

		if collection, watching := s.collectionsToWatchMap[collString]; watching {
			documentKey, isOk := changeEvent["documentKey"]
			if !isOk {
				return nil, fmt.Errorf("failed to get documentKey from change event: %v", changeEvent)
			}

			documentKeyBsonM, isOk := documentKey.(bson.M)
			if !isOk {
				return nil, fmt.Errorf("expected documentKey to be bson.M, got: %T", documentKey)
			}

			operationType, isOk := changeEvent["operationType"]
			if !isOk {
				return nil, fmt.Errorf("failed to get operationType from change event: %v", changeEvent)
			}

			operationTypeString, isOk := operationType.(string)
			if !isOk {
				return nil, fmt.Errorf("expected operationType to be string, got: %T", operationType)
			}

			var err error
			var msg *Message
			switch operationTypeString {
			case "delete":
				msg, err = ParseMessage(bson.M{"_id": documentKeyBsonM}, "d")
			case "insert":
				msg, err = ParseMessage(changeEvent["fullDocument"].(bson.M), "c")
			case "update":
				msg, err = ParseMessage(changeEvent["fullDocument"].(bson.M), "u")
			default:
				return nil, fmt.Errorf("unsupported operation type: %s", operationTypeString)
			}

			if err != nil {
				return nil, fmt.Errorf("failed to parse message: %w", err)
			}

			rawMessage, err := msg.ToRawMessage(collection, s.cfg.Database)
			if err != nil {
				return nil, fmt.Errorf("failed to convert message to raw message: %w", err)
			}

			rawMsgs = append(rawMsgs, rawMessage)
		}
	}

	if len(rawMsgs) == 0 {
		// If no messages, let's sleep for a while before checking again
		time.Sleep(1 * time.Second)
	}

	return rawMsgs, nil
}
