package temp

import (
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func ParseValue(key string, optionalSchema map[string]typing.KindDetails, val string) bool {
	if len(optionalSchema) > 0 {
		// If the column exists in the schema, let's early exit.
		if kindDetail, isOk := optionalSchema[key]; isOk {
			// If the schema exists, use it as sot.
			if kindDetail.Kind == typing.ETime.Kind || kindDetail.Kind == typing.EDecimal.Kind {
				// If the data type is either `ETime` or `EDecimal` and the value exists, we will not early exit
				// We are not skipping so that we are able to get the exact layout specified at the row level to preserve:
				// 1. Layout for time / date / timestamps
				// 2. Precision and scale for numeric values
				return ParseValue(key, nil, val)
			}

			switch kindDetail.Kind {
			case typing.String.Kind, typing.Struct.Kind, typing.ETime.Kind:
				return true
			default:
				return false
			}
		}
	}

	var convertedVal = val

	// If it contains space or -, then we must check against date time.
	// This way, we don't penalize every string into going through this loop
	// In the future, we can have specific layout RFCs run depending on the char
	if strings.Contains(convertedVal, ":") || strings.Contains(convertedVal, "-") {
		_, err := ext.ParseExtendedDateTime(convertedVal, []string{})
		if err == nil {
			return true
		}
	}

	if typing.IsJSON(convertedVal) {
		return true
	}

	return true
}
