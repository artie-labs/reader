package dynamo

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"time"
)

func NewMessageFromExport(item *dynamodb.ItemResponse, tableName string) (*Message, error) {
	//if item == nil || len(item.Item) == 0 {
	//	return nil, fmt.Errorf("item is nil or keys do not exist in this item payload")
	//}
	//
	//executionTime := time.Now()
	//op := "r"
	//return &Message{
	//	op:            op,
	//	tableName:     tableName,
	//	executionTime: executionTime,
	//	rowData:       transformNewImage(item.Item), // This is an assumed transformation function
	//	primaryKey:    transformImage(item.Keys),
	//}, nil

	return nil, nil
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

	return &Message{
		op:            op,
		tableName:     tableName,
		executionTime: executionTime,
		rowData:       transformNewImage(record.Dynamodb.NewImage),
		primaryKey:    transformNewImage(record.Dynamodb.Keys),
	}, nil
}
