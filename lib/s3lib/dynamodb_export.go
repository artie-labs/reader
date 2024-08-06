package s3lib

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

func parseDynamoDBJSON(data []byte) (map[string]types.AttributeValue, error) {
	var rawMap map[string]map[string]interface{}
	if err := json.Unmarshal(data, &rawMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON, %w", err)
	}

	dynamoMap := make(map[string]types.AttributeValue)
	for k, v := range rawMap {
		val, err := convertToAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %q: %w", k, err)
		}

		dynamoMap[k] = val
	}

	return dynamoMap, nil
}

func assertType[T any](val interface{}) (T, error) {
	castedVal, isOk := val.(T)
	if !isOk {
		var zero T
		return zero, fmt.Errorf("expected type %T, got %T", zero, val)
	}
	return castedVal, nil
}

func convertToAttributeValue(value map[string]interface{}) (types.AttributeValue, error) {
	for key, val := range value {
		switch key {
		case "S":
			castedVal, err := assertType[string](val)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberS{Value: castedVal}, nil
		case "N":
			castedVal, err := assertType[string](val)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberN{Value: castedVal}, nil
		case "B":
			castedVal, err := assertType[string](val)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberB{Value: []byte(castedVal)}, nil
		case "SS":
			listVal, isOk := val.([]any)
			if !isOk {
				return nil, fmt.Errorf("expected []any, got: %T", val)
			}

			ss := make([]string, len(listVal))
			for i, v := range listVal {
				castedValue, err := assertType[string](v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert list value: %w", err)
				}
				ss[i] = castedValue
			}
			return &types.AttributeValueMemberSS{Value: ss}, nil
		case "NS":
			listVal, isOk := val.([]any)
			if !isOk {
				return nil, fmt.Errorf("expected []any, got: %T", val)
			}

			ns := make([]string, len(listVal))
			for i, v := range listVal {
				castedValue, err := assertType[string](v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert list value: %w", err)
				}

				ns[i] = castedValue
			}

			return &types.AttributeValueMemberNS{Value: ns}, nil
		case "BS":
			listVal, isOk := val.([]any)
			if !isOk {
				return nil, fmt.Errorf("expected []any, got: %T", val)
			}

			bs := make([][]byte, len(listVal))
			for i, v := range listVal {
				castedValue, err := assertType[string](v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert list value: %w", err)
				}

				bs[i] = []byte(castedValue)
			}

			return &types.AttributeValueMemberBS{Value: bs}, nil
		case "BOOL":
			castedVal, err := assertType[bool](val)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberBOOL{Value: castedVal}, nil
		case "NULL":
			castedVal, err := assertType[bool](val)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberNULL{Value: castedVal}, nil
		case "M":
			castedMap, isOk := val.(map[string]any)
			if !isOk {
				return nil, fmt.Errorf("expected map[string]any, got: %T", val)
			}

			m := make(map[string]types.AttributeValue)
			for mapKey, mapVal := range castedMap {
				castedMapVal, isOk := mapVal.(map[string]any)
				if !isOk {
					return nil, fmt.Errorf("expected map value map[string]any, got: %T", mapVal)
				}

				convertedMapValue, err := convertToAttributeValue(castedMapVal)
				if err != nil {
					return nil, fmt.Errorf("failed to convert map value: %w", err)
				}

				m[mapKey] = convertedMapValue
			}
			return &types.AttributeValueMemberM{Value: m}, nil
		case "L":
			castedList, isOk := val.([]any)
			if !isOk {
				return nil, fmt.Errorf("expected []any{}, got: %T", val)
			}

			list := make([]types.AttributeValue, len(castedList))
			for i, lv := range val.([]interface{}) {
				castedListValue, isOk := lv.(map[string]any)
				if !isOk {
					return nil, fmt.Errorf("expected list value map[string]any, got: %T", lv)
				}

				convertedListValue, err := convertToAttributeValue(castedListValue)
				if err != nil {
					return nil, fmt.Errorf("failed to convert list value: %w", err)
				}

				list[i] = convertedListValue
			}

			return &types.AttributeValueMemberL{Value: list}, nil

		default:
			return nil, fmt.Errorf("unexpected key: %q", key)
		}
	}

	return nil, nil
}
