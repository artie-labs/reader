package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockOpts struct{}

type mockColumn = Column[int, mockOpts]

func TestByName(t *testing.T) {
	type _tc struct {
		columns     []mockColumn
		columnName  string
		expected    *mockColumn
		expectedErr string
	}

	testCases := []_tc{
		{
			columns:     []mockColumn{},
			columnName:  "col2",
			expectedErr: `no column named "col2"`,
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
			expected: &mockColumn{
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
			expectedErr: `no column named "col2"`,
		},
	}

	for _, testCase := range testCases {
		result, err := ByName(testCase.columns, testCase.columnName)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, testCase.expected, result)
		}
	}
}

func TestByNames(t *testing.T) {
	type _tc struct {
		columns     []mockColumn
		columnNames []string
		expected    []mockColumn
		expectedErr string
	}

	testCases := []_tc{
		{
			columns:     []mockColumn{},
			columnNames: []string{},
			expected:    []mockColumn(nil),
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
			expected: []mockColumn{
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
			expectedErr: `no column named "col2"`,
		},
	}

	for _, testCase := range testCases {
		result, err := ByNames(testCase.columns, testCase.columnNames)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, testCase.expected, result)
		}
	}
}

func TestFilterOutExcludedColumns(t *testing.T) {
	{
		// Empty `excludeNames`
		value, err := FilterOutExcludedColumns([]mockColumn{{Name: "foo"}}, []string{}, []string{})
		assert.NoError(t, err)
		assert.Equal(t, value, []mockColumn{{Name: "foo"}})
	}
	{
		// Non-empty `excludeNames`, excluded column is not in list
		value, err := FilterOutExcludedColumns([]mockColumn{{Name: "foo"}}, []string{"bar"}, []string{})
		assert.NoError(t, err)
		assert.Equal(t, value, []mockColumn{{Name: "foo"}})
	}
	{
		// Non-empty `excludeNames`, excluded column is in list
		value, err := FilterOutExcludedColumns([]mockColumn{{Name: "foo"}, {Name: "bar"}}, []string{"bar"}, []string{})
		assert.NoError(t, err)
		assert.Equal(t, value, []mockColumn{{Name: "foo"}})
	}
	{
		// Non-empty `excludeNames`, excluded column is in list, and is also a primary key
		_, err := FilterOutExcludedColumns([]mockColumn{{Name: "foo"}, {Name: "bar"}}, []string{"bar"}, []string{"bar"})
		assert.ErrorContains(t, err, `cannot exclude primary key column "bar"`)
	}
}

func TestFilterForIncludedColumns(t *testing.T) {
	{
		// Empty `includeNames`
		value, err := FilterForIncludedColumns([]mockColumn{{Name: "foo"}}, []string{}, []string{})
		assert.NoError(t, err)
		assert.Equal(t, value, []mockColumn{{Name: "foo"}})
	}
	{
		// Non-empty `includeNames`, included column is not in list
		value, err := FilterForIncludedColumns([]mockColumn{{Name: "foo"}}, []string{"bar"}, []string{})
		assert.NoError(t, err)
		assert.Equal(t, value, []mockColumn(nil))
	}
	{
		// Non-empty `includeNames`, included column is in list
		value, err := FilterForIncludedColumns([]mockColumn{{Name: "foo"}, {Name: "bar"}}, []string{"bar"}, []string{})
		assert.NoError(t, err)
		assert.Equal(t, value, []mockColumn{{Name: "bar"}})
	}
	{
		// Non-empty `includeNames`, included column is in list, primary key is not included
		_, err := FilterForIncludedColumns([]mockColumn{{Name: "foo"}, {Name: "bar"}}, []string{"bar"}, []string{"foo"})
		assert.ErrorContains(t, err, `primary key column "foo" must be included`)
	}
	{
		// Non-empty `includeNames`, included column is in list, primary key is included
		value, err := FilterForIncludedColumns([]mockColumn{{Name: "foo"}, {Name: "bar"}}, []string{"foo", "bar"}, []string{"foo"})
		assert.NoError(t, err)
		assert.Equal(t, value, []mockColumn{{Name: "foo"}, {Name: "bar"}})
	}
}
