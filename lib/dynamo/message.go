package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/segmentio/kafka-go"
	"strconv"
	"time"
)

type Message struct {
	rowData       map[string]interface{}
	primaryKey    map[string]interface{}
	op            string
	tableName     string
	executionTime time.Time
}

func stringToFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// transformAttributeValue converts a DynamoDB AttributeValue to a Go type.
// References: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
func transformAttributeValue(attr *dynamodb.AttributeValue) interface{} {
	switch {
	case attr.S != nil:
		return *attr.S
	case attr.N != nil:
		number, err := stringToFloat64(*attr.N)
		if err == nil {
			return number
		} else {
			// TODO - should we throw an error here?
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
		numSet := make([]float64, len(attr.NS))
		for i, n := range attr.NS {
			number, err := stringToFloat64(*n)
			if err != nil {
				// TODO - should we throw an error here?
				return nil
			}

			numSet[i] = number
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
