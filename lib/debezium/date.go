package debezium

import (
	"fmt"
	"time"
)

func ToDebeziumDate(_time interface{}) (int, error) {
	ts, isOk := _time.(time.Time)
	if !isOk {
		return 0, fmt.Errorf("object is not a time.Time object")
	}

	unix := time.UnixMilli(0).In(time.UTC) // 1970-01-01
	days := int(ts.Sub(unix).Hours() / 24)
	return days, nil
}
