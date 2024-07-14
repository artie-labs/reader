package dynamo

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// transformSnapshotAttributeValue is the same code as `transformAttributeValue`, but with different imports
// This is because the types are different, and `attributeValue` is an interface, so we can't use generics.
func transformSnapshotAttributeValue(attr any) (any, error) {
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
		for i, s := range v.Value {
			strSet[i] = s
		}
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

func transformSnapshotImage(data map[string]types.AttributeValue) (map[string]any, error) {
	transformed := make(map[string]any)
	for key, attrValue := range data {
		val, err := transformSnapshotAttributeValue(attrValue)
		if err != nil {
			return nil, fmt.Errorf("failed to transform attribute value: %w", err)
		}
		transformed[key] = val
	}

	return transformed, nil
}
