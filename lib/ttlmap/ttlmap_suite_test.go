package ttlmap

import (
	"context"
	"github.com/stretchr/testify/suite"
	"testing"
)

type TTLMapTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (t *TTLMapTestSuite) SetupTest() {
	t.ctx = context.Background()
}

func TestTTLMapTestSuite(t *testing.T) {
	suite.Run(t, new(TTLMapTestSuite))
}
