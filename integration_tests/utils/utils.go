package utils

import (
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/cdc/util"
)

func TempTableName() string {
	return fmt.Sprintf("artie_reader_%d", 10_000+rand.Int32N(5_000))
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
	fmt.Printf("Expected %s:\n", name)
	fmt.Println("--------------------------------------------------------------------------------")
	for i, line := range expectedLines {
		prefix := " "
		if i >= len(actualLines) || line != actualLines[i] {
			prefix = ">"
		}
		fmt.Println(prefix + line)
	}
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Actual %s:\n", name)
	fmt.Println("--------------------------------------------------------------------------------")
	for i, line := range actualLines {
		prefix := " "
		if i >= len(expectedLines) || line != expectedLines[i] {
			prefix = ">"
		}
		fmt.Println(prefix + line)
	}
	fmt.Println("--------------------------------------------------------------------------------")
	return true
}
