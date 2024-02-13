package postgres

import (
	"fmt"

	"github.com/artie-labs/reader/config"
)

type Connection struct {
	Host       string
	Port       uint16
	Username   string
	Password   string
	Database   string
	DisableSSL bool
}

func NewConnection(cfg config.PostgreSQL) *Connection {
	return &Connection{
		Host:       cfg.Host,
		Port:       uint16(cfg.Port),
		Username:   cfg.Username,
		Password:   cfg.Password,
		Database:   cfg.Database,
		DisableSSL: cfg.DisableSSL,
	}
}

func (c *Connection) String() string {
	connString := fmt.Sprintf("user=%s dbname=%s password=%s port=%d host=%s",
		c.Username, c.Database, c.Password, c.Port, c.Host)

	if c.DisableSSL {
		connString = fmt.Sprintf("%s sslmode=disable", connString)
	}

	return connString
}
