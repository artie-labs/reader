package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteLiteral(t *testing.T) {
	assert.Equal(t, "'abc'", QuoteLiteral("abc"))
	assert.Equal(t, "'a''bc'", QuoteLiteral("a'bc"))
}
