package streaming

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"time"
)

type Position struct {
	// Binlog position
	File string `yaml:"file"`
	Pos  uint32 `yaml:"pos"`

	// GTID set
	GTIDSet string `yaml:"gtidSet"`
	UnixTs  int64  `yaml:"unixTs"`
}

func (p Position) String() string {
	return fmt.Sprintf("File: %q, Pos: %d, GTIDSet (if enabled): %q", p.File, p.Pos, p.GTIDSet)
}

func (p Position) ToGTIDSet() (mysql.GTIDSet, error) {
	return mysql.ParseGTIDSet(mysql.MySQLFlavor, p.GTIDSet)

}

func (p Position) ToMySQLPosition() mysql.Position {
	return mysql.Position{Name: p.File, Pos: p.Pos}
}

func (p *Position) UpdatePosition(ts time.Time, evt *replication.BinlogEvent) error {
	// We should always update the log position
	p.Pos = evt.Header.LogPos
	p.UnixTs = ts.Unix()

	// If the event is a GTID event, let's set the GTID Set.
	if gtidEvent, ok := evt.Event.(*replication.GTIDEvent); ok {
		set, err := gtidEvent.GTIDNext()
		if err != nil {
			return fmt.Errorf("failed to retrieve next GTID set: %w", err)
		}

		p.GTIDSet = set.String()
	}

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