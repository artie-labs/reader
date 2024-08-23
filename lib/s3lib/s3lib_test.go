package s3lib

import (
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
		actualBucket, actualPrefix, actualErr := BucketAndPrefixFromFilePath(tc.fp)
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
