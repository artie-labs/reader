package offsets

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type OffsetsTestSuite struct {
	suite.Suite
}

func TestOffsetsTestSuite(t *testing.T) {
	suite.Run(t, new(OffsetsTestSuite))
}
