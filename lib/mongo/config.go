package mongo

import (
	"crypto/tls"
	"fmt"

	"github.com/artie-labs/reader/config"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func OptsFromConfig(cfg config.MongoDB) (*options.ClientOptions, error) {
	opts := options.Client()

	if cfg.URI != "" {
		opts = opts.ApplyURI(cfg.URI)
	} else if cfg.Host != "" {
		opts = opts.ApplyURI(cfg.Host)
		if cfg.Username != "" && cfg.Password != "" {
			opts = opts.SetAuth(options.Credential{
				Username: cfg.Username,
				Password: cfg.Password,
			})
		}
	} else {
		return nil, fmt.Errorf("mongoDB requires a URI or host")
	}

	if !cfg.DisableTLS {
		opts = opts.SetTLSConfig(&tls.Config{})
	}

	return opts, nil
}
