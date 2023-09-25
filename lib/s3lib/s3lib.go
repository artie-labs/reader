package s3lib

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

func bucketAndPrefixFromFilePath(fp string) (*string, *string, error) {
	// Remove the s3:// prefix if it's there
	fp = strings.TrimPrefix(fp, "s3://")

	parts := strings.SplitN(fp, "/", 2)
	if len(parts) < 2 {
		return nil, nil, fmt.Errorf("invalid S3 path, missing prefix")
	}

	bucket := parts[0]
	prefix := parts[1]
	return &bucket, &prefix, nil
}

func ListFiles(region, fp string) ([]string, error) {
	// Initialize a session using Amazon SDK
	sess, err := session.NewSession(&aws.Config{
		Region: ptr.ToString(region),
	})

	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)

	bucket, prefix, err := bucketAndPrefixFromFilePath(fp)
	if err != nil {
		return nil, err
	}

	var objects []string
	err = svc.ListObjectsPages(&s3.ListObjectsInput{Bucket: bucket, Prefix: prefix},
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, object := range page.Contents {
				objects = append(objects, *object.Key)
			}

			return true
		})

	if err != nil {
		return nil, err
	}

	return objects, nil
}
