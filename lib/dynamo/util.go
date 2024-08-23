package dynamo

import (
	"fmt"
	"path/filepath"
	"strings"
)

func GetTableArnFromStreamArn(streamArn string) (string, error) {
	parts := strings.Split(streamArn, "/stream/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid stream ARN: %s", streamArn)
	}

	return parts[0], nil
}

func ParseManifestFile(bucket string, manifestFilePath string) (string, error) {
	if !strings.HasSuffix(manifestFilePath, "manifest-summary.json") {
		return "", fmt.Errorf("invalid manifest filepath: %s", manifestFilePath)
	}

	parts := strings.Split(manifestFilePath, "/")
	return filepath.Join(bucket, strings.Join(parts[:len(parts)-1], "/")), nil
}
