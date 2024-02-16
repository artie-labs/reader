package debezium

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/stringutil"
	"github.com/artie-labs/transfer/lib/cdc/util"
)

func ParseValue(col schema.Column, value interface{}) (interface{}, error) {
	if value == nil {
		return value, nil
	}

	var err error
	switch col.Type {
	case schema.Timestamp:
		valTime, isOk := value.(time.Time)
		if isOk {
			if valTime.Year() > 9999 || valTime.Year() < 0 {
				// Avoid copying this column over because it'll cause a JSON Marshal error:
				// Time.MarshalJSON: year outside of range [0,9999]
				slog.Info("Skipping timestamp because year is greater than 9999 or less than 0", slog.String("key", col.Name), slog.Any("value", value))
				return nil, nil
			}
		}
	case schema.Date:
		value, err = debezium.ToDebeziumDate(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date for key %s: %w", col.Name, err)
		}

	case schema.Numeric, schema.Money:
		if col.Type == schema.Money {
			value = stringutil.ParseMoneyIntoString(fmt.Sprint(value))
		}

		scale, err := strconv.Atoi(*col.Opts.Scale)
		if err != nil {
			return nil, fmt.Errorf("unable to find scale for key: %s: %w", col.Name, err)
		}

		value, err = debezium.EncodeDecimalToBase64(fmt.Sprint(value), scale)
		if err != nil {
			return nil, fmt.Errorf("failed to encode decimal to b64 for key %s: %w", col.Name, err)
		}
	case schema.VariableNumeric:
		encodedValue, err := debezium.EncodeDecimalToBase64(fmt.Sprint(value), debezium.GetScale(fmt.Sprint(value)))
		if err != nil {
			return util.SchemaEventPayload{}, fmt.Errorf("failed to encode decimal to b64 for key %s: %w", col.Name, err)
		}

		value = map[string]string{
			"scale": fmt.Sprint(debezium.GetScale(fmt.Sprint(value))),
			"value": encodedValue,
		}
	}

	return value, nil
}
