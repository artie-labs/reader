package s3lib

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	bucketName *string
	client     *s3.Client
}

func NewClient(bucketName string, awsCfg aws.Config) *S3Client {
	return &S3Client{
		bucketName: &bucketName,
		client:     s3.NewFromConfig(awsCfg),
	}
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

type S3File struct {
	Key *string `yaml:"key"`
}

func (s *S3Client) ListFiles(ctx context.Context, fp string) ([]S3File, error) {
	bucket, prefix, err := bucketAndPrefixFromFilePath(fp)
	if err != nil {
		return nil, err
	}

	var files []S3File
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, object := range page.Contents {
			files = append(files, S3File{
				Key: object.Key,
			})
		}
	}
	return files, nil
}

// StreamJsonGzipFile - will take a S3 File that is in `json.gz` format from DynamoDB's export to S3
// It's not a typical JSON file in that it is compressed and it's new line delimited via separated via an array
// Which means we can stream this file row by row to not OOM.
func (s *S3Client) StreamJsonGzipFile(ctx context.Context, file S3File, ch chan<- map[string]types.AttributeValue) error {
	const maxBufferSize = 1024 * 1024 // 1 MB or adjust as needed

	defer close(ch)
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: s.bucketName,
		Key:    file.Key,
	})

	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}

	defer result.Body.Close()

	// Create a gzip reader
	gz, err := gzip.NewReader(result.Body)
	if err != nil {
		return fmt.Errorf("failed to create a GZIP reader for object: %w", err)
	}

	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	buf := make([]byte, maxBufferSize)
	scanner.Buffer(buf, maxBufferSize)

	for scanner.Scan() {
		line := scanner.Bytes()
		output, err := parseDynamoDBJSON(line)
		if err != nil {
			return fmt.Errorf("failed to parse dynamodb json: %w", err)
		}

		ch <- output
	}

	if err = scanner.Err(); err != nil {
		return fmt.Errorf("error reading from S3 object: %w", err)
	}

	return nil
}
