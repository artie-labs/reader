package mongo

import (
	"crypto/tls"
	"fmt"

	"github.com/artie-labs/reader/config"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func OptsFromConfig(cfg config.MongoDB) (*options.ClientOptions, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("mongoDB requires a URI")
	}

	opts := options.Client().ApplyURI(cfg.URI)

	if !cfg.DisableTLS {
		opts = opts.SetTLSConfig(&tls.Config{})
	}

	return opts, nil
}
