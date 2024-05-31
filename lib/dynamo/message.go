package dynamo

import (
	"fmt"
	"strconv"
	"time"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/aws/aws-sdk-go/service/dynamodb"

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
func transformAttributeValue(attr *dynamodb.AttributeValue) (any, error) {
	switch {
	case attr.S != nil:
		return *attr.S, nil
	case attr.N != nil:
		number, err := stringToFloat64(*attr.N)
		if err == nil {
			return number, nil
		}

		return nil, fmt.Errorf("failed to convert string to float64: %w", err)
	case attr.BOOL != nil:
		return *attr.BOOL, nil
	case attr.M != nil:
		result := make(map[string]any)
		for k, v := range attr.M {
			val, err := transformAttributeValue(v)
			if err != nil {
				return nil, fmt.Errorf("failed to transform attribute value: %w", err)
			}

			result[k] = val
		}

		return result, nil
	case attr.L != nil:
		list := make([]any, len(attr.L))
		for i, item := range attr.L {
			val, err := transformAttributeValue(item)
			if err != nil {
				return nil, fmt.Errorf("failed to transform attribute value: %w", err)
			}

			list[i] = val
		}

		return list, nil
	case attr.SS != nil:
		// Convert the string set to a slice of strings.
		strSet := make([]string, len(attr.SS))
		for i, s := range attr.SS {
			strSet[i] = *s
		}

		return strSet, nil
	case attr.NS != nil:
		// Convert the number set to a slice of strings (since the numbers are stored as strings).
		numSet := make([]float64, len(attr.NS))
		for i, n := range attr.NS {
			number, err := stringToFloat64(*n)
			if err != nil {
				return nil, fmt.Errorf("failed to convert string to float64: %w", err)
			}

			numSet[i] = number
		}

		return numSet, nil
	}

	return nil, nil
}

func transformImage(data map[string]*dynamodb.AttributeValue) (map[string]any, error) {
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
