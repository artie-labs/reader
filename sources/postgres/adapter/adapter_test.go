package adapter

import (
	"testing"

	"github.com/artie-labs/reader/lib/postgres"
	"github.com/stretchr/testify/assert"
)

func TestPostgresAdapter_TopicSuffix(t *testing.T) {
	type _tc struct {
		table             postgres.Table
		expectedTopicName string
	}

	tcs := []_tc{
		{
			table: postgres.Table{
				Name:   "table1",
				Schema: "schema1",
			},
			expectedTopicName: "schema1.table1",
		},
		{
			table: postgres.Table{
				Name:   `"PublicStatus"`,
				Schema: "schema2",
			},
			expectedTopicName: "schema2.PublicStatus",
		},
	}

	for _, tc := range tcs {
		adapter := NewPostgresAdapter(tc.table)
		assert.Equal(t, tc.expectedTopicName, adapter.TopicSuffix())
	}
}
