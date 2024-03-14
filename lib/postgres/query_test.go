package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuotedIdentifiers(t *testing.T) {
	assert.Equal(t, []string{`"a"`, `"bb""bb"`, `"c"`}, QuotedIdentifiers([]string{"a", `bb"bb`, "c"}))
}

func TestQueryPlaceholders(t *testing.T) {
	assert.Equal(t, []string{}, QueryPlaceholders(0, 0))
	assert.Equal(t, []string{"$1", "$2"}, QueryPlaceholders(0, 2))
	assert.Equal(t, []string{"$4", "$5", "$6", "$7"}, QueryPlaceholders(3, 4))
}
