package debezium

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewArgs_Validate(t *testing.T) {
	type _tc struct {
		name      string
		newArgs   *NewArgs
		expectErr bool
	}

	tcs := []_tc{
		{
			name:      "nil newArgs",
			expectErr: true,
		},
		{
			name:      "newArgs is set",
			newArgs:   &NewArgs{},
			expectErr: true,
		},
		{
			name: "newArgs, tableName is set",
			newArgs: &NewArgs{
				TableName: "hello",
			},
			expectErr: true,
		},
		{
			name: "newArgs, tableName and columns is set",
			newArgs: &NewArgs{
				TableName: "hello",
				Columns:   []string{"a", "b"},
			},
			expectErr: true,
		},
		{
			name: "newArgs, tableName, columns and rowData is set",
			newArgs: &NewArgs{
				TableName: "hello",
				Columns:   []string{"a", "b"},
				RowData: map[string]interface{}{
					"a": 1,
				},
			},
			expectErr: true,
		},
		{
			name: "newArgs, tableName, columns, rowData and config is set",
			newArgs: &NewArgs{
				TableName: "hello",
				Columns:   []string{"a", "b"},
				RowData: map[string]interface{}{
					"a": 1,
				},
				Fields: NewFields(),
			},
		},
	}

	for _, tc := range tcs {
		actualErr := tc.newArgs.Validate()
		if tc.expectErr {
			assert.Error(t, actualErr, tc.name)
		} else {
			assert.NoError(t, actualErr, tc.name)
		}
	}
}

func TestNewPayload_NilOptionalSchema(t *testing.T) {
	rowData := map[string]interface{}{
		"user_id": 123,
		"name":    "Robin",
	}

	columns := []string{"user_id", "name"}

	payload, err := NewPayload(&NewArgs{
		TableName: "foo",
		Columns:   columns,
		Fields:    NewFields(),
		RowData:   rowData,
	})
	assert.NotNil(t, payload)
	assert.NoError(t, err)

	assert.Equal(t, "r", payload.Payload.Operation)
	assert.Equal(t, rowData, payload.Payload.After)
	assert.Equal(t, "foo", payload.GetTableName())
}
