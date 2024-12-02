package transfer

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/debezium/transformer"
)

type mockAdapter struct {
	fieldConverters []transformer.FieldConverter
	partitionKeys   []string
}

func (m mockAdapter) FieldConverters() []transformer.FieldConverter {
	return m.fieldConverters
}

func (m mockAdapter) PartitionKeys() []string {
	return m.partitionKeys
}

func TestBuildTransferColumns(t *testing.T) {
	adapter := mockAdapter{
		partitionKeys: []string{"id"},
		fieldConverters: []transformer.FieldConverter{
			{
				Name:           "id",
				ValueConverter: converters.StringPassthrough{},
			},
			{
				Name:           "name",
				ValueConverter: converters.StringPassthrough{},
			},
		},
	}

	cols, err := BuildTransferColumns(adapter)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(cols))
	assert.Equal(t, "id", cols[0].Name())
	assert.Equal(t, typing.String, cols[0].KindDetails)
	assert.True(t, cols[0].PrimaryKey())

	assert.Equal(t, "name", cols[1].Name())
	assert.Equal(t, typing.String, cols[1].KindDetails)
	assert.False(t, cols[1].PrimaryKey())
}
