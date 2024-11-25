package antlr

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDropTable(t *testing.T) {
	{
		// DROP TABLE `table_name`;
		events, err := Parse("DROP TABLE `table_name`;")
		fmt.Println("event", events, "err", err)
		assert.NoError(t, err)

		for _, evt := range events {
			fmt.Println("event", evt.GetTables())
		}

		assert.False(t, true)
	}
}
