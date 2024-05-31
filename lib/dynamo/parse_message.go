package dynamo

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

func NewMessageFromExport(item dynamodb.ItemResponse, keys []string, tableName string) (*Message, error) {
	if len(item.Item) == 0 {
		return nil, fmt.Errorf("item is nil or keys do not exist in this item payload")
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("keys is nil")
	}

	// Snapshot time does not exist on the row
	// Perhaps we can have it inferred from the manifest file in the future.
	rowData, err := transformImage(item.Item)
	if err != nil {
		return nil, fmt.Errorf("failed to transform item: %w", err)
	}

	primaryKeys := make(map[string]any)
	for _, key := range keys {
		val, isOk := rowData[key]
		if !isOk {
			return nil, fmt.Errorf("key does not exist in the item payload")
		}

		primaryKeys[key] = val
	}

	return &Message{
		op:            "r",
		tableName:     tableName,
		executionTime: time.Now(),
		afterRowData:  rowData,
		primaryKey:    primaryKeys,
	}, nil
}

func NewMessage(record *dynamodbstreams.Record, tableName string) (*Message, error) {
	if record == nil || record.Dynamodb == nil {
		return nil, fmt.Errorf("record is nil or dynamodb does not exist in this event payload")
	}

	if len(record.Dynamodb.Keys) == 0 {
		return nil, fmt.Errorf("keys is nil")
	}

	executionTime := time.Now()
	if record.Dynamodb.ApproximateCreationDateTime != nil {
		executionTime = *record.Dynamodb.ApproximateCreationDateTime
	}

	op := "r"
	if record.EventName != nil {
		switch *record.EventName {
		case "INSERT":
			op = "c"
		case "MODIFY":
			op = "u"
		case "REMOVE":
			op = "d"
		}
	}

	beforeData, err := transformImage(record.Dynamodb.OldImage)
	if err != nil {
		return nil, fmt.Errorf("failed to transform old image: %w", err)
	}

	afterData, err := transformImage(record.Dynamodb.NewImage)
	if err != nil {
		return nil, fmt.Errorf("failed to transform new image: %w", err)
	}

	primaryKey, err := transformImage(record.Dynamodb.Keys)
	if err != nil {
		return nil, fmt.Errorf("failed to transform keys: %w", err)
	}

	return &Message{
		op:            op,
		tableName:     tableName,
		executionTime: executionTime,
		beforeRowData: beforeData,
		afterRowData:  afterData,
		primaryKey:    primaryKey,
	}, nil
}
