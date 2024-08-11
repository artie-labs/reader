package mongo

import (
	"crypto/tls"
	"github.com/artie-labs/reader/config"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func OptsFromConfig(cfg config.MongoDB) *options.ClientOptions {
	opts := options.Client().ApplyURI(cfg.Host)
	if !cfg.DisableTLS {
		opts = opts.SetTLSConfig(&tls.Config{})
	}

	if cfg.Username != "" && cfg.Password != "" {
		opts = opts.SetAuth(options.Credential{
			Username: cfg.Username,
			Password: cfg.Password,
		})
	}

	return opts
}
