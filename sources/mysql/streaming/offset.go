package streaming

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type Position struct {
	// Binlog position
	File string `yaml:"file"`
	Pos  uint32 `yaml:"pos"`
	// GTID
	GTIDSet string `yaml:"gtidSet"`

	// Generated
	_gtidSet mysql.GTIDSet `yaml:"-"`

	UnixTs int64 `yaml:"unixTs"`
}

func (p Position) String() string {
	return fmt.Sprintf("File: %q, Pos: %d, GTIDSet (if enabled): %q", p.File, p.Pos, p.GTIDSet)
}

func (p *Position) ToGTIDSet() (mysql.GTIDSet, error) {
	_gtidSet, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, p.GTIDSet)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GTID set: %w", err)
	}

	p._gtidSet = _gtidSet
	return _gtidSet, nil
}

func (p Position) ToMySQLPosition() mysql.Position {
	return mysql.Position{Name: p.File, Pos: p.Pos}
}

func (p *Position) UpdatePosition(ts time.Time, evt *replication.BinlogEvent) error {
	// We should always update the log position
	p.Pos = evt.Header.LogPos
	p.UnixTs = ts.Unix()
	if evt.Header.EventType == replication.GTID_EVENT {
		gtidEvent, err := typing.AssertType[*replication.GTIDEvent](evt.Event)
		if err != nil {
			return err
		}

		set, err := gtidEvent.GTIDNext()
		if err != nil {
			return fmt.Errorf("failed to retrieve next GTID set: %w", err)
		}

		if err = p._gtidSet.Update(set.String()); err != nil {
			return fmt.Errorf("failed to update GTID set: %w", err)
		}

		p.GTIDSet = p._gtidSet.String()
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
