package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuotedIdentifiers(t *testing.T) {
	assert.Equal(t, []string{`"a"`, `"bb""bb"`, `"c"`}, QuotedIdentifiers([]string{"a", `bb"bb`, "c"}))
}
