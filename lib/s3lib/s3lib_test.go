package s3lib

import (
	"fmt"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestBucketAndPrefixFromFilePath(t *testing.T) {
	tcs := []struct {
		name           string
		fp             string
		expectedBucket *string
		expectedPrefix *string
		expectedErr    string
	}{
		{
			name:           "valid path (w/ S3 prefix)",
			fp:             "s3://bucket/prefix",
			expectedBucket: ptr.ToString("bucket"),
			expectedPrefix: ptr.ToString("prefix"),
		},
		{
			name:           "valid path (w/ S3 prefix) with trailing slash",
			fp:             "s3://bucket/prefix/",
			expectedBucket: ptr.ToString("bucket"),
			expectedPrefix: ptr.ToString("prefix/"),
		},
		{
			name:           "valid path (w/ S3 prefix) with multiple slashes",
			fp:             "s3://bucket/prefix/with/multiple/slashes",
			expectedBucket: ptr.ToString("bucket"),
			expectedPrefix: ptr.ToString("prefix/with/multiple/slashes"),
		},
		// Without S3 prefix
		{
			name:           "valid path (w/o S3 prefix)",
			fp:             "bucket/prefix",
			expectedBucket: ptr.ToString("bucket"),
			expectedPrefix: ptr.ToString("prefix"),
		},
		{
			name:           "valid path (w/o S3 prefix) with trailing slash",
			fp:             "bucket/prefix/",
			expectedBucket: ptr.ToString("bucket"),
			expectedPrefix: ptr.ToString("prefix/"),
		},
		{
			name:           "valid path (w/o S3 prefix) with multiple slashes",
			fp:             "bucket/prefix/with/multiple/slashes",
			expectedBucket: ptr.ToString("bucket"),
			expectedPrefix: ptr.ToString("prefix/with/multiple/slashes"),
		},
		{
			name:        "invalid path",
			fp:          "s3://bucket",
			expectedErr: "invalid S3 path, missing prefix",
		},
	}

	for _, tc := range tcs {
		actualBucket, actualPrefix, actualErr := bucketAndPrefixFromFilePath(tc.fp)
		if tc.expectedErr != "" {
			assert.ErrorContains(t, actualErr, tc.expectedErr, tc.name)
		} else {
			assert.NoError(t, actualErr, tc.name)

			// Now check the actualBucket and prefix
			assert.Equal(t, *tc.expectedBucket, *actualBucket, tc.name)
			assert.Equal(t, *tc.expectedPrefix, *actualPrefix, tc.name)
		}
	}
}

func TestUnmarshalDynamoDBExportItem(t *testing.T) {
	body := `{"Item":{"account_id":{"S":"account-9825"},"user_id":{"S":"user_id_1"},"is_null":{"NULL":true},"sample_list":{"L":[{"S":"item1"},{"N":"2"}]},"flag":{"BOOL":false},"string_set":{"SS":["value2","value44","value55","value66","value1"]},"random_number":{"N":"4851973137566368817"},"number_set":{"NS":["3","2","1"]},"sample_map":{"M":{"key1":{"S":"value1"},"key2":{"N":"2"}}}}}`
	item, err := unmarshalDynamoDBExport([]byte(body))
	assert.NoError(t, err)
	assert.Equal(t, "account-9825", item["account_id"].(*ddbTypes.AttributeValueMemberS).Value)
	assert.Equal(t, "user_id_1", item["user_id"].(*ddbTypes.AttributeValueMemberS).Value)

	for key, value := range item {
		fmt.Println("key", key, "value", value, fmt.Sprintf("value type: %T", value))
	}

	//fmt.Println("itemIsNull", item["is_null"], fmt.Sprintf("Type: %T", item["is_null"]))
	//assert.Nil(t, item["is_null"])

	assert.Equal(t, false, item["flag"].(*ddbTypes.AttributeValueMemberBOOL).Value)
	assert.Equal(t, "4851973137566368817", item["random_number"].(*ddbTypes.AttributeValueMemberN).Value)
	assert.Equal(t, []string{"item1", "2"}, item["sample_list"].(*ddbTypes.AttributeValueMemberL).Value)
	assert.Equal(t, []string{"value2", "value44", "value55", "value66", "value1"}, item["string_set"].(*ddbTypes.AttributeValueMemberSS).Value)
}
