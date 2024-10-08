package s3lib

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

type exportedPayload struct {
	Item map[string]map[string]any `json:"Item"`
}

func parseDynamoDBJSON(data []byte) (map[string]types.AttributeValue, error) {
	var payload exportedPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON, %w", err)
	}

	dynamoMap := make(map[string]types.AttributeValue)
	for k, v := range payload.Item {
		val, err := convertToAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %q: %w", k, err)
		}

		dynamoMap[k] = val
	}

	return dynamoMap, nil
}

// convertToAttributeValue converts a map[string]any in DynamoDB encoding into types.AttributeValue.
// This is necessary because the serialization from DynamoDB JSON to map[string]types.AttributeValue was removed from v1 -> v2 of the SDK.
// See discussion: https://github.com/aws/aws-sdk-go-v2/discussions/1652
func convertToAttributeValue(value map[string]any) (types.AttributeValue, error) {
	for key, val := range value {
		switch key {
		case "S":
			castedVal, err := typing.AssertType[string](val)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberS{Value: castedVal}, nil
		case "N":
			castedVal, err := typing.AssertType[string](val)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberN{Value: castedVal}, nil
		case "B":
			castedVal, err := typing.AssertType[string](val)
			if err != nil {
				return nil, err
			}

			decodedBytes, err := base64.StdEncoding.DecodeString(castedVal)
			if err != nil {
				return nil, fmt.Errorf("failed to base64 decode: %w", err)
			}

			return &types.AttributeValueMemberB{Value: decodedBytes}, nil
		case "SS":
			listVal, isOk := val.([]any)
			if !isOk {
				return nil, fmt.Errorf("expected []any, got: %T", val)
			}

			ss := make([]string, len(listVal))
			for i, v := range listVal {
				castedValue, err := typing.AssertType[string](v)
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
				castedValue, err := typing.AssertType[string](v)
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
				castedValue, err := typing.AssertType[string](v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert list value: %w", err)
				}

				decodedBytes, err := base64.StdEncoding.DecodeString(castedValue)
				if err != nil {
					return nil, fmt.Errorf("failed to base64 decode: %w", err)
				}

				bs[i] = decodedBytes
			}

			return &types.AttributeValueMemberBS{Value: bs}, nil
		case "BOOL":
			castedVal, err := typing.AssertType[bool](val)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberBOOL{Value: castedVal}, nil
		case "NULL":
			castedVal, err := typing.AssertType[bool](val)
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
			for i, lv := range castedList {
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
