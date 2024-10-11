package mongo

import (
	"testing"

	"github.com/artie-labs/reader/config"
	"github.com/stretchr/testify/assert"
)

func TestOptsFromConfig(t *testing.T) {
	{
		// Disable TLS
		cfg := config.MongoDB{
			URI:        "mongodb://user:pass@localhost",
			DisableTLS: true,
		}

		opts, err := OptsFromConfig(cfg)
		assert.NoError(t, err)
		assert.Nil(t, opts.TLSConfig)
	}
	{
		// Using URI:
		cfg := config.MongoDB{
			URI: "mongodb://user:pass@localhost",
		}

		opts, err := OptsFromConfig(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, opts.TLSConfig)
		assert.Equal(t, "user", opts.Auth.Username)
		assert.Equal(t, "pass", opts.Auth.Password)
	}
}
