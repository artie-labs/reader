package dynamo

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/artie-labs/reader/lib"
)

type Message struct {
	beforeRowData map[string]any
	afterRowData  map[string]any
	primaryKey    map[string]any
	afterSchema   map[string]debezium.FieldType
	op            string
	tableName     string
	executionTime time.Time
}

func stringToFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// transformAttributeValue converts a DynamoDB AttributeValue to a Go type.
// References: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
func transformAttributeValue(attr types.AttributeValue) (any, debezium.FieldType, error) {
	switch v := attr.(type) {
	case *types.AttributeValueMemberS:
		return v.Value, debezium.String, nil
	case *types.AttributeValueMemberN:
		number, err := stringToFloat64(v.Value)
		if err != nil {
			return nil, "", fmt.Errorf("failed to convert string to float64: %w", err)
		}
		return number, debezium.Float, nil
	case *types.AttributeValueMemberB:
		return v.Value, debezium.Bytes, nil
	case *types.AttributeValueMemberBS:
		return v.Value, debezium.Array, nil
	case *types.AttributeValueMemberBOOL:
		return v.Value, debezium.Boolean, nil
	case *types.AttributeValueMemberM:
		result := make(map[string]any)
		for k, v := range v.Value {
			val, _, err := transformAttributeValue(v)
			if err != nil {
				return nil, "", fmt.Errorf("failed to transform attribute value: %w", err)
			}
			result[k] = val
		}
		return result, debezium.Map, nil
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			val, _, err := transformAttributeValue(item)
			if err != nil {
				return nil, "", fmt.Errorf("failed to transform attribute value: %w", err)
			}
			list[i] = val
		}
		return list, debezium.Array, nil
	case *types.AttributeValueMemberSS:
		return slices.Clone(v.Value), debezium.Array, nil
	case *types.AttributeValueMemberNS:
		numSet := make([]float64, len(v.Value))
		for i, n := range v.Value {
			number, err := stringToFloat64(n)
			if err != nil {
				return nil, "", fmt.Errorf("failed to convert string to float64: %w", err)
			}
			numSet[i] = number
		}
		return numSet, debezium.Array, nil
	}

	return nil, "", nil
}

func transformImage(data map[string]types.AttributeValue) (map[string]any, map[string]debezium.FieldType, error) {
	keyToFieldMap := make(map[string]debezium.FieldType)
	transformed := make(map[string]any)
	for key, attrValue := range data {
		val, field, err := transformAttributeValue(attrValue)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to transform attribute value: %w", err)
		}

		keyToFieldMap[key] = field
		transformed[key] = val
	}

	return transformed, keyToFieldMap, nil
}

func (m *Message) artieMessage() *util.SchemaEventPayload {
	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{},
	}

	if len(m.afterSchema) > 0 {
		var fields []debezium.Field
		for colName, fieldType := range m.afterSchema {
			fields = append(fields, debezium.Field{
				Type:      fieldType,
				Optional:  true,
				FieldName: colName,
			})
		}

		schema.FieldsObject = append(schema.FieldsObject, debezium.FieldsObject{
			FieldLabel: debezium.After,
			Fields:     fields,
		})
	}

	return &util.SchemaEventPayload{
		Schema: schema,
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
