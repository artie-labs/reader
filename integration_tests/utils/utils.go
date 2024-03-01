package utils

import (
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/util"
)

func TempTableName() string {
	return fmt.Sprintf("artie_reader_%d", 10_000+rand.Int32N(10_000))
}

func GetPayload(message lib.RawMessage) util.SchemaEventPayload {
	payloadTyped, ok := message.GetPayload().(util.SchemaEventPayload)
	if !ok {
		panic("payload is not of type util.SchemaEventPayload")
	}
	return payloadTyped
}

func CheckDifference(name, expected, actual string) bool {
	if expected == actual {
		return false
	}
	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")
	fmt.Println("--------------------------------------------------------------------------------")
	for i := range max(len(expectedLines), len(actualLines)) {
		if i < len(expectedLines) {
			if i < len(actualLines) {
				if expectedLines[i] == actualLines[i] {
					fmt.Println(expectedLines[i])
				} else {
					fmt.Println("E" + expectedLines[i])
					fmt.Println("A" + actualLines[i])
				}
			} else {
				fmt.Println("E" + expectedLines[i])
			}
		} else {
			fmt.Println("A" + actualLines[i])
		}
	}
	fmt.Println("--------------------------------------------------------------------------------")
	return true
}
