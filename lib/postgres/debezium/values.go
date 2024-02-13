package debezium

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/stringutil"
	"github.com/artie-labs/transfer/lib/cdc/util"
)

func ParseValue(key string, value interface{}, fields *Fields) (interface{}, error) {
	if value == nil {
		return value, nil
	}

	var err error
	dt := fields.GetDataType(key)
	switch dt {
	case Timestamp:
		valTime, isOk := value.(time.Time)
		if isOk {
			if valTime.Year() > 9999 || valTime.Year() < 0 {
				// Avoid copying this column over because it'll cause a JSON Marshal error:
				// Time.MarshalJSON: year outside of range [0,9999]
				slog.Info("Skipping timestamp because year is greater than 9999 or less than 0", slog.String("key", key), slog.Any("value", value))
				return nil, nil
			}
		}
	case Date:
		value, err = debezium.ToDebeziumDate(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date for key %s: %w", key, err)
		}

	case Numeric, Money:
		if dt == Money {
			value = stringutil.ParseMoneyIntoString(fmt.Sprint(value))
		}

		field, isOk := fields.GetField(key)
		if !isOk {
			return nil, fmt.Errorf("unable to find field, key: %v", key)
		}

		scale, err := strconv.Atoi(fmt.Sprint(field.Parameters["scale"]))
		if err != nil {
			return nil, fmt.Errorf("unable to find scale for key: %s: %w", key, err)
		}

		value, err = debezium.EncodeDecimalToBase64(fmt.Sprint(value), scale)
		if err != nil {
			return nil, fmt.Errorf("failed to encode decimal to b64 for key %s: %w", key, err)
		}
	case VariableNumeric:
		encodedValue, err := debezium.EncodeDecimalToBase64(fmt.Sprint(value), debezium.GetScale(fmt.Sprint(value)))
		if err != nil {
			return util.SchemaEventPayload{}, fmt.Errorf("failed to encode decimal to b64 for key %s: %w", key, err)
		}

		value = map[string]string{
			"scale": fmt.Sprint(debezium.GetScale(fmt.Sprint(value))),
			"value": encodedValue,
		}
	}

	return value, nil
}
