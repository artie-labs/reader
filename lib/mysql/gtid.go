package mysql

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"strconv"
	"strings"
)

func ShouldProcessRow(set mysql.GTIDSet, currentGTID string) (bool, error) {
	gtidSet, ok := set.(*mysql.MysqlGTIDSet)
	if !ok {
		return false, fmt.Errorf("unsupported GTID set type: %T", set)
	}

	if len(gtidSet.Sets) == 0 {
		// We have not seen any GTIDs yet, so we should process this one.
		return true, nil
	}

	gtidParts := strings.Split(currentGTID, ":")
	if len(gtidParts) != 2 {
		return false, fmt.Errorf("invalid GTID format: %q", currentGTID)
	}

	sid := gtidParts[0]
	seenIntervals, ok := gtidSet.Sets[sid]
	if !ok {
		// We have not seen this SID before, so we should process it.
		return true, nil
	}

	txID, err := strconv.ParseInt(gtidParts[1], 10, 64)
	if err != nil {
		return false, fmt.Errorf("failed to parse transaction ID: %w", err)
	}

	var highestTxID int64
	for _, interval := range seenIntervals.Intervals {
		if interval.Stop > highestTxID {
			highestTxID = interval.Stop
		}
	}

	// We should process if the current txID is above or equal to the highest txID we have seen
	return txID >= highestTxID, nil
}
