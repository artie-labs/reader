package mysql

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func getGTID(sid uuid.UUID, txID int64) string {
	return fmt.Sprintf("%s:%d", sid.String(), txID)
}

func TestShouldProcessRow(t *testing.T) {
	{
		// GTID is not set, should still return
		shouldProcess, err := ShouldProcessRow(nil, "foo:1")
		assert.NoError(t, err)
		assert.True(t, shouldProcess)
	}
	{
		// Nothing defined for the set, so we should process it.
		set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
		assert.NoError(t, err)

		shouldProcess, err := ShouldProcessRow(set, "foo:1")
		assert.NoError(t, err)
		assert.True(t, shouldProcess)
	}
	{
		// We have seen the SID before, but the txID is higher than the highest we have seen.
		sid := uuid.New()
		set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, getGTID(sid, 1))
		assert.NoError(t, err)

		shouldProcess, err := ShouldProcessRow(set, getGTID(sid, 2))
		assert.NoError(t, err)
		assert.True(t, shouldProcess)
	}
	{
		// There's more than one SID pre-existing
		sid1 := uuid.New()
		sid2 := uuid.New()
		set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, strings.Join([]string{getGTID(sid1, 1), getGTID(sid2, 1)}, ","))
		assert.NoError(t, err)

		shouldProcess, err := ShouldProcessRow(set, getGTID(sid1, 2))
		assert.NoError(t, err)
		assert.True(t, shouldProcess)

		shouldProcess, err = ShouldProcessRow(set, getGTID(sid2, 2))
		assert.NoError(t, err)
		assert.True(t, shouldProcess)
	}
	{
		// We have not seen the SID before
		sid := uuid.New()
		set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, getGTID(sid, 1))
		assert.NoError(t, err)

		shouldProcess, err := ShouldProcessRow(set, getGTID(uuid.New(), 1))
		assert.NoError(t, err)
		assert.True(t, shouldProcess)
	}
}
