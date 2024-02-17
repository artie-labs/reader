package temp

import (
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func ParseValue(settings typing.Settings, key string, optionalSchema map[string]typing.KindDetails, val string) typing.KindDetails {
	if len(optionalSchema) > 0 {
		// If the column exists in the schema, let's early exit.
		if kindDetail, isOk := optionalSchema[key]; isOk {
			// If the schema exists, use it as sot.
			if kindDetail.Kind == typing.ETime.Kind || kindDetail.Kind == typing.EDecimal.Kind {
				// If the data type is either `ETime` or `EDecimal` and the value exists, we will not early exit
				// We are not skipping so that we are able to get the exact layout specified at the row level to preserve:
				// 1. Layout for time / date / timestamps
				// 2. Precision and scale for numeric values
				return ParseValue(settings, key, nil, val)
			}

			return kindDetail
		}
	}

	var convertedVal = val

	// If it contains space or -, then we must check against date time.
	// This way, we don't penalize every string into going through this loop
	// In the future, we can have specific layout RFCs run depending on the char
	if strings.Contains(convertedVal, ":") || strings.Contains(convertedVal, "-") {
		extendedKind, err := ext.ParseExtendedDateTime(convertedVal, settings.AdditionalDateFormats)
		if err == nil {
			return typing.KindDetails{
				Kind:                typing.ETime.Kind,
				ExtendedTimeDetails: &extendedKind.NestedKind,
			}
		}
	}

	if typing.IsJSON(convertedVal) {
		return typing.Struct
	}

	return typing.String
}
