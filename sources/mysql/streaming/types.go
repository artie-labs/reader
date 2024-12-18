package streaming

import (
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/storage/persistedlist"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/streaming/ddl"
)

type Iterator struct {
	cfg       config.MySQL
	batchSize int32
	position  Position

	offsets           *persistedmap.PersistedMap[Position]
	schemaHistoryList *persistedlist.PersistedList[SchemaHistory]

	schemaAdapter *ddl.SchemaAdapter
	streamer      *replication.BinlogStreamer
	syncer        *replication.BinlogSyncer
}

type SchemaHistory struct {
	Query  string `json:"query"`
	UnixTs int64  `json:"unixTs"`
}
