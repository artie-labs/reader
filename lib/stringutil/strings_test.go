package stringutil

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseMoneyIntoString(t *testing.T) {
	type _testCase struct {
		name          string
		money         string
		expectedValue string
	}

	testCases := []_testCase{
		{
			name:          "happy path",
			money:         "$4,000",
			expectedValue: "4000",
		},
		{
			name:          "happy path (millions)",
			money:         "$4,000,000",
			expectedValue: "4000000",
		},
		{
			name:          "happy path (hundreds)",
			money:         "$400",
			expectedValue: "400",
		},
		{
			name:          "happy path (hundreds)",
			money:         "$400.55",
			expectedValue: "400.55",
		},
		{
			name:          "nothing changed",
			money:         "999",
			expectedValue: "999",
		},
	}

	for _, testCase := range testCases {
		actualMoney := ParseMoneyIntoString(testCase.money)
		assert.Equal(t, actualMoney, testCase.expectedValue, testCase.name)
	}
}
