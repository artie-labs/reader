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
	// fullDocumentBeforeChange (optional) is only present if the db + collection enabled `changeStreamPreAndPostImages`
	fullDocumentBeforeChange *bson.M
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
		switch castedFullDoc := fullDoc.(type) {
		case bson.M:
			changeEvent.fullDocument = &castedFullDoc
		case nil:
			// This may happen if:
			// t0: Updated document A
			// t1: Deleted document A
			// t2: Looking up the after object for document A
			// https://www.mongodb.com/community/forums/t/how-can-change-stream-update-operations-come-with-null-fulldocument-when-changestreamfulldocumentoption-updatelookup-was-used/2537/5
			changeEvent.fullDocument = &bson.M{
				"_id": objectID,
			}
		default:
			return nil, fmt.Errorf("expected fullDocument to be bson.M or nil, got: %T", fullDoc)
		}
	}

	fullDocumentBeforeChange, isOk := rawChangeEvent["fullDocumentBeforeChange"]
	if isOk {
		switch castedFullDoc := fullDocumentBeforeChange.(type) {
		case bson.M:
			changeEvent.fullDocumentBeforeChange = &castedFullDoc
		case nil:
			// This may happen if the row was purged before we can read it
			changeEvent.fullDocumentBeforeChange = &bson.M{
				"_id": objectID,
			}
		default:
			return nil, fmt.Errorf("expected fullDocumentBeforeChange to be bson.M or nil, got: %T", fullDoc)
		}
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
		msg, err := ParseMessage(bson.M{"_id": c.objectID}, c.fullDocumentBeforeChange, "d")
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}

		return msg, nil
	case "insert":
		fullDocument, err := c.getFullDocument()
		if err != nil {
			return nil, fmt.Errorf("failed to get fullDocument from change event: %v", c)
		}

		msg, err := ParseMessage(fullDocument, nil, "c")
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}

		return msg, nil
	case "update", "replace":
		fullDocument, err := c.getFullDocument()
		if err != nil {
			return nil, fmt.Errorf("failed to get fullDocument from change event: %v", c)
		}

		msg, err := ParseMessage(fullDocument, c.fullDocumentBeforeChange, "u")
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}

		return msg, nil
	default:
		return nil, fmt.Errorf("unsupported operation type: %q", c.operationType)
	}
}
