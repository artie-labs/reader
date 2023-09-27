package s3lib

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

type S3Client struct {
	client *s3.S3
}

func NewClient(region string) (*S3Client, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: ptr.ToString(region),
	})

	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)
	return &S3Client{client: svc}, nil
}

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

func (s *S3Client) ListFiles(fp string) ([]string, error) {
	bucket, prefix, err := bucketAndPrefixFromFilePath(fp)
	if err != nil {
		return nil, err
	}

	var objects []string
	err = s.client.ListObjectsPages(&s3.ListObjectsInput{Bucket: bucket, Prefix: prefix},
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

func StreamJsonGZFile() {

}
