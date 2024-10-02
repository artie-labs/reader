package s3lib

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucketAndPrefixFromFilePath(t *testing.T) {
	{
		// Invalid
		{
			// Empty string
			bucket, prefix, err := BucketAndPrefixFromFilePath("")
			assert.ErrorContains(t, err, "invalid S3 path, missing prefix")
			assert.Empty(t, bucket)
			assert.Empty(t, prefix)
		}
		{
			// Bucket only, no prefix
			bucket, prefix, err := BucketAndPrefixFromFilePath("bucket")
			assert.ErrorContains(t, err, "invalid S3 path, missing prefix")
			assert.Empty(t, bucket)
			assert.Empty(t, prefix)
		}
	}
	{
		// Valid
		{
			// No S3 prefix
			bucket, prefix, err := BucketAndPrefixFromFilePath("bucket/prefix")
			assert.NoError(t, err)
			assert.Equal(t, "bucket", bucket)
			assert.Equal(t, "prefix", prefix)
		}
		{
			// S3 prefix
			bucket, prefix, err := BucketAndPrefixFromFilePath("s3://bucket/prefix")
			assert.NoError(t, err)
			assert.Equal(t, "bucket", bucket)
			assert.Equal(t, "prefix", prefix)
		}
	}
}
