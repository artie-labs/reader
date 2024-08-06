package mongo

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
)

type ChangeEvent struct {
	operationType string
	collection    string
	objectID      any

	fullDocument *bson.M
}

func NewChangeEvent(rawChangeEvent bson.M) (*ChangeEvent, error) {
	operationType, isOk := rawChangeEvent["operationType"]
	if !isOk {
		return nil, fmt.Errorf("failed to get operationType from change event: %v", rawChangeEvent)
	}

	castedOperationType, isOk := operationType.(string)
	if !isOk {
		return nil, fmt.Errorf("expected operationType to be string, got: %T", operationType)
	}

	documentKey, isOk := rawChangeEvent["documentKey"]
	if !isOk {
		return nil, fmt.Errorf("failed to get documentKey from change event: %v", rawChangeEvent)
	}

	castedDocumentKey, isOk := documentKey.(bson.M)
	if !isOk {
		return nil, fmt.Errorf("expected documentKey to be bson.M, got: %T", documentKey)
	}

	ns, isOk := rawChangeEvent["ns"]
	if !isOk {
		return nil, fmt.Errorf("failed to get namespace from change event: %v", rawChangeEvent)
	}

	nsBsonM, isOk := ns.(bson.M)
	if !isOk {
		return nil, fmt.Errorf("expected ns to be bson.M, got: %T", ns)
	}

	coll, isOk := nsBsonM["coll"]
	if !isOk {
		return nil, fmt.Errorf("failed to get collection from change event: %v", rawChangeEvent)
	}

	collString, isOk := coll.(string)
	if !isOk {
		return nil, fmt.Errorf("expected collection to be string, got: %T", coll)
	}

	objectID, isOk := castedDocumentKey["_id"]
	if !isOk {
		return nil, fmt.Errorf("failed to get _id from documentKey: %v", castedDocumentKey)
	}

	changeEvent := &ChangeEvent{
		operationType: castedOperationType,
		collection:    collString,
		objectID:      objectID,
	}

	fullDoc, isOk := rawChangeEvent["fullDocument"]
	if isOk {
		castedFullDocument, isOk := fullDoc.(bson.M)
		if !isOk {
			return nil, fmt.Errorf("expected fullDocument to be bson.M, got: %T", fullDoc)
		}

		changeEvent.fullDocument = &castedFullDocument
	}

	return changeEvent, nil
}

func (c ChangeEvent) Collection() string {
	return c.collection
}

func (c ChangeEvent) getFullDocument() (bson.M, error) {
	if c.fullDocument == nil {
		return nil, fmt.Errorf("fullDocument is not present")
	}

	return *c.fullDocument, nil
}

func (c ChangeEvent) ToMessage() (*Message, error) {
	switch c.operationType {
	case "delete":
		// TODO: Think about providing the `before` row for a deleted event.
		msg, err := ParseMessage(bson.M{"_id": c.objectID}, "d")
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}

		return msg, nil
	case "insert":
		fullDocument, err := c.getFullDocument()
		if err != nil {
			return nil, fmt.Errorf("failed to get fullDocument from change event: %v", c)
		}

		msg, err := ParseMessage(fullDocument, "c")
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}

		return msg, nil
	case "update":
		fullDocument, err := c.getFullDocument()
		if err != nil {
			return nil, fmt.Errorf("failed to get fullDocument from change event: %v", c)
		}

		msg, err := ParseMessage(fullDocument, "u")
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}

		return msg, nil
	default:
		return nil, fmt.Errorf("unsupported operation type: %q", c.operationType)
	}
}
