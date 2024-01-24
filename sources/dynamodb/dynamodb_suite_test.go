package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type DynamoDBTestSuite struct {
	suite.Suite
}

func TestDynamoDBTestSuite(t *testing.T) {
	suite.Run(t, new(DynamoDBTestSuite))
}
