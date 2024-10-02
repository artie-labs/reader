package dynamo

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTableArnFromStreamArn(t *testing.T) {
	{
		// Valid stream ARN
		tableArn, err := GetTableArnFromStreamArn("arn:aws:dynamodb:us-west-2:123456789012:table/my-table/stream/2021-01-01T00:00:00.000")
		assert.NoError(t, err)
		assert.Equal(t, "arn:aws:dynamodb:us-west-2:123456789012:table/my-table", tableArn)
	}
	{
		// Invalid stream ARN
		_, err := GetTableArnFromStreamArn("arn:aws:dynamodb:us-west-2:123456789012:table/my-table")
		assert.ErrorContains(t, err, `invalid stream ARN: "arn:aws:dynamodb:us-west-2:123456789012:table/my-table"`)
	}
}

func TestParseManifestFile(t *testing.T) {
	{
		// Valid manifest file path
		bucket, err := ParseManifestFile("bucket", "artie-ddb-export/AWSDynamoDB/abcdef-8831c8f6/manifest-summary.json")
		assert.NoError(t, err)
		assert.Equal(t, "bucket/artie-ddb-export/AWSDynamoDB/abcdef-8831c8f6", bucket)
	}
	{
		// Invalid manifest file path
		_, err := ParseManifestFile("bucket", "artie-ddb-export/AWSDynamoDB/abcdef-8831c8f6/manifest-summary")
		assert.ErrorContains(t, err, `invalid manifest filepath: "artie-ddb-export/AWSDynamoDB/abcdef-8831c8f6/manifest-summary"`)
	}
}
