package mysql

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetHighWatermark(t *testing.T) {
	{
		// Nothing defined for the set
		set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
		assert.NoError(t, err)

		below, err := GetHighWatermark(set, "foo:1")


	}
}
