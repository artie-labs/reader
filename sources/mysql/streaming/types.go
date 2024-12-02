package streaming

import (
	"fmt"
	"github.com/artie-labs/reader/lib/storage/persistedmap"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type Iterator struct {
	batchSize int32
	position  Position
	offsets   *persistedmap.PersistedMap[Position]
	streamer  *replication.BinlogStreamer
	syncer    *replication.BinlogSyncer
}

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
