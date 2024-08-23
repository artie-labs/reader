package snapshot

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log/slog"
	"time"
)

func (s *Store) findRecentExport(ctx context.Context, s3FilePath string) (*string, *string, error) {
	tableARN, err := dynamo.GetTableArnFromStreamArn(s.streamArn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get table ARN from stream ARN: %w", err)
	}

	exports, err := s.listExports(ctx, tableARN)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list exports: %w", err)
	}

	for _, export := range exports {
		if export.ExportStatus == types.ExportStatusFailed {
			slog.Info("Filtering out failed export", slog.String("exportARN", *export.ExportArn))
			continue
		}

		exportDescription, err := s.dynamoDBClient.DescribeExport(ctx, &dynamodb.DescribeExportInput{ExportArn: export.ExportArn})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to describe export: %w", err)
		}

		if *exportDescription.ExportDescription.S3Bucket == s.s3BucketName && *exportDescription.ExportDescription.S3Prefix == s.s3PrefixName {
			if export.ExportStatus == types.ExportStatusCompleted {
				return export.ExportArn, exportDescription.ExportDescription.ExportManifest, nil
			}

			return export.ExportArn, nil, nil
		}
	}

	return nil, nil, fmt.Errorf("no recent export found for %s", s3FilePath)
}

func (s *Store) listExports(ctx context.Context, tableARN string) ([]types.ExportSummary, error) {
	var out []types.ExportSummary

	var nextToken *string
	for {
		exports, err := s.dynamoDBClient.ListExports(ctx, &dynamodb.ListExportsInput{
			TableArn:  aws.String(tableARN),
			NextToken: nextToken,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to list exports: %w", err)
		}

		out = append(out, exports.ExportSummaries...)
		if exports.NextToken == nil {
			break
		}

		nextToken = exports.NextToken
	}

	return out, nil
}

func (s *Store) checkExportStatus(ctx context.Context, exportARN *string) (*string, error) {
	for {
		result, err := s.dynamoDBClient.DescribeExport(ctx, &dynamodb.DescribeExportInput{ExportArn: exportARN})
		if err != nil {
			return nil, fmt.Errorf("failed to describe export: %w", err)
		}

		switch result.ExportDescription.ExportStatus {
		case types.ExportStatusCompleted:
			return result.ExportDescription.ExportManifest, nil
		case types.ExportStatusFailed:
			return nil, fmt.Errorf("export has failed: %s", *result.ExportDescription.FailureMessage)
		case types.ExportStatusInProgress:
			slog.Info("Export is still in progress")
			time.Sleep(30 * time.Second)
		default:
			return nil, fmt.Errorf("unknown export status: %s", string(result.ExportDescription.ExportStatus))
		}
	}
}
