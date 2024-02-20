package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuotedIdentifiers(t *testing.T) {
	assert.Equal(t, []string{`"a"`, `"bb""bb"`, `"c"`}, QuotedIdentifiers([]string{"a", `bb"bb`, "c"}))
}

func TestQuoteLiteral(t *testing.T) {
	assert.Equal(t, "'abc'", QuoteLiteral("abc"))
	assert.Equal(t, "'a''bc'", QuoteLiteral("a'bc"))
}
