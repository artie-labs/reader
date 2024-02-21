package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteIdentifiers(t *testing.T) {
	assert.Equal(t, []string{`"a"`, `"bb""bb"`, `"c"`}, QuoteIdentifiers([]string{"a", `bb"bb`, "c"}))
}

func TestQuoteLiteral(t *testing.T) {
	assert.Equal(t, "'abc'", QuoteLiteral("abc"))
	assert.Equal(t, "'a''bc'", QuoteLiteral("a'bc"))
}
