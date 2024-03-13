package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockOpts struct{}

type mockColumn = Column[int, mockOpts]

func TestGetColumnByName(t *testing.T) {
	type _tc struct {
		columns        []mockColumn
		columnName     string
		expectedResult *mockColumn
		expectedErr    string
	}

	testCases := []_tc{
		{
			columns:     []mockColumn{},
			columnName:  "col2",
			expectedErr: "failed to find column with name: col2",
		},
		{

			columns: []mockColumn{
				{
					Name: "col1",
					Type: 1,
				},
				{
					Name: "col2",
					Type: 2,
				},
				{
					Name: "col3",
					Type: 3,
				},
			},
			columnName: "col2",
			expectedResult: &mockColumn{
				Name: "col2",
				Type: 2,
			},
		},
		{
			columns: []mockColumn{
				{
					Name: "col1",
					Type: 1,
				},
			},
			columnName:  "col2",
			expectedErr: "failed to find column with name: col2",
		},
	}

	for _, testCase := range testCases {
		result, err := GetColumnByName(testCase.columns, testCase.columnName)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, testCase.expectedResult, result)
		}
	}
}

func TestGetColumnsByName(t *testing.T) {
	type _tc struct {
		columns        []mockColumn
		columnNames    []string
		expectedResult []mockColumn
		expectedErr    string
	}

	testCases := []_tc{
		{
			columns:        []mockColumn{},
			columnNames:    []string{},
			expectedResult: []mockColumn(nil),
		},
		{
			columns: []mockColumn{
				{
					Name: "col1",
					Type: 1,
				},
				{
					Name: "col2",
					Type: 2,
				},
				{
					Name: "col3",
					Type: 3,
				},
			},
			columnNames: []string{"col1", "col3"},
			expectedResult: []mockColumn{
				{
					Name: "col1",
					Type: 1,
				},
				{
					Name: "col3",
					Type: 3,
				},
			},
		},
		{
			columns: []mockColumn{
				{
					Name: "col1",
					Type: 1,
				},
			},
			columnNames: []string{"col1", "col2"},
			expectedErr: "failed to find column with name: col2",
		},
	}

	for _, testCase := range testCases {
		result, err := GetColumnsByName(testCase.columns, testCase.columnNames)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, testCase.expectedResult, result)
		}
	}
}
