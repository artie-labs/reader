package streaming

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type Position struct {
	File string `yaml:"file"`
	Pos  uint32 `yaml:"pos"`
}

func (p Position) String() string {
	return fmt.Sprintf("File: %s, Pos: %d", p.File, p.Pos)
}

func (p Position) ToMySQLPosition() mysql.Position {
	return mysql.Position{Name: p.File, Pos: p.Pos}
}
