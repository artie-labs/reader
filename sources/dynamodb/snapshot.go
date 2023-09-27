package dynamodb

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/logger"
)

func (s *Store) scanFilesOverBucket() error {
	files, err := s.s3Client.ListFiles(s.cfg.SnapshotSettings.Folder)
	if err != nil {
		return fmt.Errorf("failed to list files, err: %v", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no files found in the folder: %v", s.cfg.SnapshotSettings.Folder)
	}

	s.cfg.SnapshotSettings.SpecifiedFiles = files
	return nil
}

func (s *Store) ReadAndPublish(ctx context.Context) error {
	for _, file := range s.cfg.SnapshotSettings.SpecifiedFiles {
		logger.FromContext(ctx).Info("processing file: ", file)
	}

	return nil
}
