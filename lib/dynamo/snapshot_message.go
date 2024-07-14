package dynamo

import (
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

func convertSnapshotToStreamingAttribute(attr ddbTypes.AttributeValue) types.AttributeValue {
	switch v := attr.(type) {
	case *ddbTypes.AttributeValueMemberS:
		return &types.AttributeValueMemberS{Value: v.Value}
	case *ddbTypes.AttributeValueMemberN:
		return &types.AttributeValueMemberN{Value: v.Value}
	case *ddbTypes.AttributeValueMemberBOOL:
		return &types.AttributeValueMemberBOOL{Value: v.Value}
	case *ddbTypes.AttributeValueMemberM:
		result := make(map[string]types.AttributeValue)
		for k, v := range v.Value {
			val := convertSnapshotToStreamingAttribute(v)
			result[k] = val
		}

		return &types.AttributeValueMemberM{Value: result}
	case *ddbTypes.AttributeValueMemberL:
		list := make([]types.AttributeValue, len(v.Value))
		for i, item := range v.Value {
			val := convertSnapshotToStreamingAttribute(item)
			list[i] = val
		}

		return &types.AttributeValueMemberL{Value: list}
	case *ddbTypes.AttributeValueMemberSS:
		return &types.AttributeValueMemberSS{Value: v.Value}
	case *ddbTypes.AttributeValueMemberNS:
		return &types.AttributeValueMemberNS{Value: v.Value}
	}

	return nil
}

// TODO: Add tests
func convertSnapshotToStreamingImage(data map[string]ddbTypes.AttributeValue) map[string]types.AttributeValue {
	transformed := make(map[string]types.AttributeValue)
	for key, attrValue := range data {
		val := convertSnapshotToStreamingAttribute(attrValue)
		transformed[key] = val
	}

	return transformed
}
