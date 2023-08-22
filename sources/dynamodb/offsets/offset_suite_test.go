package offsets

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/stretchr/testify/suite"
	"testing"
)

type OffsetsTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (o *OffsetsTestSuite) SetupTest() {
	o.ctx = context.Background()
	o.ctx = config.InjectIntoContext(o.ctx, &config.Settings{})
}

func TestOffsetsTestSuite(t *testing.T) {
	suite.Run(t, new(OffsetsTestSuite))
}
