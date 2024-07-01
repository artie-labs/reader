package mongo

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/config"
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
	collectionsToWatchMap map[string]bool
	offsets               *persistedmap.PersistedMap
}

func newStreamingIterator(ctx context.Context, db *mongo.Database, cfg config.MongoDB, filePath string) (*streaming, error) {
	collectionsToWatchMap := make(map[string]bool)
	for _, collection := range cfg.Collections {
		collectionsToWatchMap[collection.Name] = true
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
	var messages []lib.RawMessage

	// Check for new events
	if s.changeStream.Next(s.ctx) {
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

		if _, watching := s.collectionsToWatchMap[collString]; watching {

		}

		// Filter events based on collectionsToWatchMap
		if ns, ok := changeEvent["ns"].(bson.M); ok {
			if collection, ok := ns["coll"].(string); ok {
				if _, watching := s.collectionsToWatchMap[collection]; watching {
					rawMessage := lib.RawMessage{
						Data: changeEvent,
					}
					messages = append(messages, rawMessage)
				}
			}
		}
	}

	if len(messages) == 0 {
		// If no messages, let's sleep for a while before checking again
		time.Sleep(1 * time.Second)
	}

	return messages, nil
}
