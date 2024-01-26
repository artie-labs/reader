package stringutil

import (
	"fmt"
	"strings"
)

// ParseMoneyIntoString will change $4,000 to 4000
func ParseMoneyIntoString(money string) string {
	retVal := strings.Replace(fmt.Sprint(money), "$", "", 1)
	retVal = strings.ReplaceAll(retVal, ",", "")
	return retVal
}
