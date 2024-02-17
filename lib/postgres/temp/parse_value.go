package temp

import (
	"github.com/artie-labs/transfer/lib/typing"
)

func ParseValue(key string, optionalSchema map[string]typing.KindDetails) bool {
	if len(optionalSchema) > 0 {
		// If the column exists in the schema, let's early exit.
		if kindDetail, isOk := optionalSchema[key]; isOk {
			switch kindDetail.Kind {
			case typing.String.Kind, typing.Struct.Kind, typing.ETime.Kind, typing.EDecimal.Kind:
				return true
			default:
				return false
			}
		}
	}

	return true
}
