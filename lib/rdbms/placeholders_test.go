package rdbms

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueryPlaceholders(t *testing.T) {
	assert.Equal(t, []string{}, QueryPlaceholders("?", 0))
	assert.Equal(t, []string{"?"}, QueryPlaceholders("?", 1))
	assert.Equal(t, []string{"?", "?"}, QueryPlaceholders("?", 2))
}
