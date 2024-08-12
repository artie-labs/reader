package mongo

import (
	"github.com/artie-labs/reader/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptsFromConfig(t *testing.T) {
	{
		cfg := config.MongoDB{
			Host:     "localhost",
			Username: "user",
			Password: "password",
		}

		opts := OptsFromConfig(cfg)
		assert.NotNil(t, opts.TLSConfig)
		assert.Equal(t, "user", opts.Auth.Username)
		assert.Equal(t, "password", opts.Auth.Password)
	}
	{
		// No username and password
		cfg := config.MongoDB{
			Host: "localhost",
		}

		opts := OptsFromConfig(cfg)
		assert.Nil(t, opts.Auth)
	}
	{
		// Disable TLS
		cfg := config.MongoDB{
			Host:       "localhost",
			DisableTLS: true,
		}

		opts := OptsFromConfig(cfg)
		assert.Nil(t, opts.TLSConfig)
	}
}
