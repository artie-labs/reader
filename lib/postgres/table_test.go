package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicSuffix(t *testing.T) {
	type _tc struct {
		table             *Table
		expectedTopicName string
	}

	tcs := []_tc{
		{
			table: &Table{
				Name:   "table1",
				Schema: "schema1",
			},
			expectedTopicName: "schema1.table1",
		},
		{
			table: &Table{
				Name:   `"PublicStatus"`,
				Schema: "schema2",
			},
			expectedTopicName: "schema2.PublicStatus",
		},
	}

	for _, tc := range tcs {
		assert.Equal(t, tc.expectedTopicName, tc.table.TopicSuffix())
	}
}
