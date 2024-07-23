package s3lib

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	client *s3.Client
}

func NewClient(awsCfg aws.Config) *S3Client {
	return &S3Client{client: s3.NewFromConfig(awsCfg)}
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
				Key:    object.Key,
				Bucket: bucket,
			})
		}
	}

	return files, nil
}

// StreamJsonGzipFile will take an S3 File that is in `json.gz` format from DynamoDB's export to S3.
// It's not a typical JSON file in that it is compressed and it's new line delimited via an array,
// which means we can stream this file row by row to not OOM.
func (s *S3Client) StreamJsonGzipFile(ctx context.Context, file S3File, ch chan<- map[string]ddbTypes.AttributeValue) error {
	defer close(ch)
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: file.Bucket,
		Key:    file.Key,
	})

	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}

	defer result.Body.Close()

	gz, err := gzip.NewReader(result.Body)
	if err != nil {
		return fmt.Errorf("failed to create a GZIP reader for object: %w", err)
	}

	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		avMap, err := unmarshalDynamoDBExport(scanner.Bytes())
		if err != nil {
			return fmt.Errorf("failed to unmarshal DynamoDB export: %w", err)
		}

		ch <- avMap
	}

	if err = scanner.Err(); err != nil {
		return fmt.Errorf("error reading from S3 object: %w", err)
	}

	return nil
}

func unmarshalDynamoDBExport(item []byte) (map[string]ddbTypes.AttributeValue, error) {
	type ddbItemExport struct {
		Item map[string]any `json:"Item"`
	}

	var export ddbItemExport
	if err := json.Unmarshal(item, &export); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	attributeValue, err := attributevalue.Marshal(export.Item)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal attribute value: %w", err)
	}

	castedAttrValue, isOk := attributeValue.(*ddbTypes.AttributeValueMemberM)
	if !isOk {
		return nil, fmt.Errorf("attributeValue is not type *ddbTypes.AttributeValueMemberM")
	}

	avMap := make(map[string]ddbTypes.AttributeValue)
	for key, value := range castedAttrValue.Value {
		attrValueM, isOk := value.(*ddbTypes.AttributeValueMemberM)
		if !isOk {
			return nil, fmt.Errorf("value is not type *ddbTypes.AttributeValueMemberM")
		}

		for _, castedValue := range attrValueM.Value {
			// We need to break out of the loop because DynamoDB JSON looks like: {"key": {"S": "value"}} and {"key": "value"}
			avMap[key] = castedValue
			break
		}
	}

	return avMap, nil
}
