package s3lib

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Client struct {
	client *s3.S3
}

func NewClient(session *session.Session) *S3Client {
	return &S3Client{client: s3.New(session)}
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
	Bucket *string `yaml:"bucket"`
	Key    *string `yaml:"key"`
}

func (s *S3Client) ListFiles(fp string) ([]S3File, error) {
	bucket, prefix, err := bucketAndPrefixFromFilePath(fp)
	if err != nil {
		return nil, err
	}

	var files []S3File
	err = s.client.ListObjectsPages(&s3.ListObjectsInput{Bucket: bucket, Prefix: prefix},
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, object := range page.Contents {
				files = append(files, S3File{
					Key:    object.Key,
					Bucket: bucket,
				})
			}

			return true
		})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// StreamJsonGzipFile - will take a S3 File that is in `json.gz` format from DynamoDB's export to S3
// It's not a typical JSON file in that it is compressed and it's new line delimited via separated via an array
// Which means we can stream this file row by row to not OOM.
func (s *S3Client) StreamJsonGzipFile(file S3File, ch chan<- dynamodb.ItemResponse) error {
	defer close(ch)

	result, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: file.Bucket,
		Key:    file.Key,
	})
	if err != nil {
		return fmt.Errorf("failed to get object from S3, err: %v", err)
	}

	defer result.Body.Close()

	buf := &bytes.Buffer{}
	_, err = buf.ReadFrom(result.Body)
	if err != nil {
		return fmt.Errorf("failed to read from S3 object, err: %v", err)
	}

	// Create a gzip reader
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return fmt.Errorf("failed to create a GZIP reader for object, err: %v", err)
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		line := scanner.Bytes()
		var content dynamodb.ItemResponse
		if err := json.Unmarshal(line, &content); err != nil {
			return fmt.Errorf("failed to unmarshal, err: %v", err)
		}

		ch <- content
	}

	if err = scanner.Err(); err != nil {
		return fmt.Errorf("error reading from S3 object, err: %v", err)
	}

	return nil
}
