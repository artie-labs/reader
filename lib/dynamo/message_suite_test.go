package dynamo

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/stretchr/testify/suite"
)

type DynamoDBTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (d *DynamoDBTestSuite) SetupTest() {
	d.ctx = context.Background()
	d.ctx = config.InjectIntoContext(d.ctx, &config.Settings{})
}
