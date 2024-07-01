package mongo

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
)

type ChangeEvent struct {
	operationType string
	documentKey   bson.M
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
		documentKey:   castedDocumentKey,
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

func (c ChangeEvent) ObjectID() any {
	return c.objectID
}

func (c ChangeEvent) Operation() string {
	return c.operationType
}

func (c ChangeEvent) Collection() string {
	return c.collection
}

func (c ChangeEvent) FullDocument() (bson.M, error) {
	if c.fullDocument == nil {
		return nil, fmt.Errorf("fullDocument is not present")
	}

	return *c.fullDocument, nil
}
