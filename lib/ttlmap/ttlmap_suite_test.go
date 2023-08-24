package ttlmap

import (
	"context"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/stretchr/testify/suite"
	"testing"
)

type TTLMapTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (t *TTLMapTestSuite) SetupTest() {
	ctx := config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: true,
		Config: &config.Config{
			Redshift: &config.Redshift{},
		},
	})

	t.ctx = ctx
}

func TestTTLMapTestSuite(t *testing.T) {
	suite.Run(t, new(TTLMapTestSuite))
}
