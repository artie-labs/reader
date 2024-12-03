package streaming

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/lib/storage/persistedlist"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
)

type Iterator struct {
	batchSize int32
	position  Position

	offsets           *persistedmap.PersistedMap[Position]
	schemaHistoryList *persistedlist.PersistedList[SchemaHistory]

	schemaAdapter *SchemaAdapter
	streamer      *replication.BinlogStreamer
	syncer        *replication.BinlogSyncer
}

type SchemaHistory struct {
	Query string    `json:"query"`
	Ts    time.Time `json:"ts"`
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

func (p *Position) UpdatePosition(evt *replication.BinlogEvent) error {
	// We should always update the log position
	p.Pos = evt.Header.LogPos
	if evt.Header.EventType == replication.ROTATE_EVENT {
		// When we encounter a rotate event, we'll then update the log file
		rotate, err := typing.AssertType[*replication.RotateEvent](evt.Event)
		if err != nil {
			return err
		}

		p.File = string(rotate.NextLogName)
	}

	return nil
}
