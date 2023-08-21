package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/segmentio/kafka-go"
	"strconv"
	"time"
)

const maxPublishCount = 5

type Message struct {
	rowData       map[string]interface{}
	primaryKey    map[string]interface{}
	op            string
	tableName     string
	executionTime time.Time
}

func transformAttributeValue(attr *dynamodb.AttributeValue) interface{} {
	switch {
	case attr.S != nil:
		return *attr.S
	case attr.N != nil:
		if num, err := strconv.ParseFloat(*attr.N, 64); err == nil {
			return num
		} else {
			// TODO: Should we throw an error here?
			return nil
		}
	case attr.BOOL != nil:
		return *attr.BOOL
	case attr.M != nil:
		result := make(map[string]interface{})
		for k, v := range attr.M {
			result[k] = transformAttributeValue(v)
		}
		return result
	case attr.L != nil:
		list := make([]interface{}, len(attr.L))
		for i, item := range attr.L {
			list[i] = transformAttributeValue(item)
		}
		return list
	case attr.SS != nil:
		// Convert the string set to a slice of strings.
		strSet := make([]string, len(attr.SS))
		for i, s := range attr.SS {
			strSet[i] = *s
		}
		return strSet
	case attr.NS != nil:
		// Convert the number set to a slice of strings (since the numbers are stored as strings).
		numSet := make([]string, len(attr.NS))
		for i, n := range attr.NS {
			numSet[i] = *n
		}
		return numSet
	}

	return nil
}

func transformNewImage(data map[string]*dynamodb.AttributeValue) map[string]interface{} {
	transformed := make(map[string]interface{})
	for key, attrValue := range data {
		transformed[key] = transformAttributeValue(attrValue)
	}
	return transformed
}

func NewMessage(record *dynamodbstreams.Record, tableName string) (*Message, error) {
	if record == nil && record.Dynamodb == nil {
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

func (m *Message) artieMessage() (util.SchemaEventPayload, error) {
	return util.SchemaEventPayload{
		Payload: util.Payload{
			After: m.rowData,
			Source: util.Source{
				TsMs:  m.executionTime.UnixMilli(),
				Table: m.tableName,
			},
			Operation: m.op,
		},
	}, nil
}

func (m *Message) TopicName(ctx context.Context) (string, error) {
	cfg := config.FromContext(ctx)
	if cfg.Kafka == nil {
		return "", fmt.Errorf("kafka config is nil")
	}

	return fmt.Sprintf("%s.%s", cfg.Kafka.TopicPrefix, m.tableName), nil
}

func (m *Message) KafkaMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := m.artieMessage()
	if err != nil {
		return kafka.Message{}, fmt.Errorf("failed to generate artie message, err: %v", err)
	}

	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return kafka.Message{}, err
	}

	keyBytes, err := json.Marshal(m.primaryKey)
	if err != nil {
		return kafka.Message{}, err
	}

	topic, err := m.TopicName(ctx)
	if err != nil {
		return kafka.Message{}, err
	}

	return kafka.Message{
		Topic: topic,
		Key:   keyBytes,
		Value: jsonBytes,
	}, nil
}
