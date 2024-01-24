package ttlmap

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TTLMapTestSuite struct {
	suite.Suite
}

func TestTTLMapTestSuite(t *testing.T) {
	suite.Run(t, new(TTLMapTestSuite))
}
