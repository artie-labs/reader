package dynamo

import (
	"fmt"
	"strconv"
	"time"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/artie-labs/reader/lib"
)

type Message struct {
	beforeRowData map[string]any
	afterRowData  map[string]any
	primaryKey    map[string]any
	op            string
	tableName     string
	executionTime time.Time
}

func stringToFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// transformAttributeValue converts a DynamoDB AttributeValue to a Go type.
// References: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
func transformAttributeValue(attr types.AttributeValue) (any, error) {
	switch v := attr.(type) {
	case *types.AttributeValueMemberS:
		return v.Value, nil
	case *types.AttributeValueMemberN:
		number, err := stringToFloat64(v.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert string to float64: %w", err)
		}
		return number, nil
	case *types.AttributeValueMemberBOOL:
		return v.Value, nil
	case *types.AttributeValueMemberM:
		result := make(map[string]any)
		for k, v := range v.Value {
			val, err := transformAttributeValue(v)
			if err != nil {
				return nil, fmt.Errorf("failed to transform attribute value: %w", err)
			}
			result[k] = val
		}
		return result, nil
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			val, err := transformAttributeValue(item)
			if err != nil {
				return nil, fmt.Errorf("failed to transform attribute value: %w", err)
			}
			list[i] = val
		}
		return list, nil
	case *types.AttributeValueMemberSS:
		strSet := make([]string, len(v.Value))
		copy(strSet, v.Value)
		return strSet, nil
	case *types.AttributeValueMemberNS:
		numSet := make([]float64, len(v.Value))
		for i, n := range v.Value {
			number, err := stringToFloat64(n)
			if err != nil {
				return nil, fmt.Errorf("failed to convert string to float64: %w", err)
			}
			numSet[i] = number
		}
		return numSet, nil
	}

	return nil, nil
}

func transformImage(data map[string]types.AttributeValue) (map[string]any, error) {
	transformed := make(map[string]any)
	for key, attrValue := range data {
		val, err := transformAttributeValue(attrValue)
		if err != nil {
			return nil, fmt.Errorf("failed to transform attribute value: %w", err)
		}
		transformed[key] = val
	}

	return transformed, nil
}

func (m *Message) artieMessage() *util.SchemaEventPayload {
	return &util.SchemaEventPayload{
		Payload: util.Payload{
			Before: m.beforeRowData,
			After:  m.afterRowData,
			Source: util.Source{
				TsMs:  m.executionTime.UnixMilli(),
				Table: m.tableName,
			},
			Operation: m.op,
		},
	}
}

func (m *Message) RawMessage() lib.RawMessage {
	return lib.NewRawMessage(m.tableName, m.primaryKey, m.artieMessage())
}
