package mssql

import (
    "database/sql"
    "fmt"
    "slices"
    "strings"
    "time"

    "github.com/artie-labs/transfer/clients/mssql/dialect"

    "github.com/artie-labs/reader/lib/mssql/parse"
    "github.com/artie-labs/reader/lib/mssql/schema"
    "github.com/artie-labs/reader/lib/rdbms"
    "github.com/artie-labs/reader/lib/rdbms/column"
    "github.com/artie-labs/reader/lib/rdbms/primary_key"
    "github.com/artie-labs/reader/lib/rdbms/scan"
)

const (
    TimeMicro      = "15:04:05.000000"
    TimeNano       = "15:04:05.000000000"
    DateTimeMicro  = "2006-01-02 15:04:05.000000"
    DateTimeNano   = "2006-01-02 15:04:05.000000000"
    DateTimeOffset = "2006-01-02 15:04:05.0000000 -07:00"
)

func NewScanner(db *sql.DB, table Table, columns []schema.Column, cfg scan.ScannerConfig) (*scan.Scanner, error) {
    for _, key := range table.PrimaryKeys() {
        if _, err := column.ByName(columns, key); err != nil {
            return nil, fmt.Errorf("missing column with name: %q", key)
        }
    }

    primaryKeyBounds, err := table.FetchPrimaryKeysBounds(db)
    if err != nil {
        return nil, err
    }

    adapter := scanAdapter{schema: table.Schema, tableName: table.Name, columns: columns}
    return scan.NewScanner(db, primaryKeyBounds, cfg, adapter)
}

type scanAdapter struct {
    schema    string
    tableName string
    columns   []schema.Column
}

func (s scanAdapter) ParsePrimaryKeyValueForOverrides(_ string, value string) (any, error) {
    // We don't need to cast it at all.
    return value, nil
}

// parsePrimaryKeyValues - parse primary key values based on the column type.
// This is needed because the MSSQL SDK does not support parsing `time.Time`, so we need to do it ourselves.
func (s scanAdapter) parsePrimaryKeyValues(columnName string, value any) (any, error) {
    columnIdx := slices.IndexFunc(s.columns, func(x schema.Column) bool { return x.Name == columnName })
    if columnIdx < 0 {
        return nil, fmt.Errorf("primary key column does not exist: %q", columnName)
    }
	
    switch _columnType := s.columns[columnIdx].Type; _columnType {
    case schema.Time:
        switch castedVal := value.(type) {
        case time.Time:
            return castedVal.Format(time.TimeOnly), nil
        case string:
            return castedVal, nil
        default:
            return nil, fmt.Errorf("expected time.Time type, received: %T", value)
        }
    case schema.TimeMicro:
        switch castedVal := value.(type) {
        case time.Time:
            return castedVal.Format(TimeMicro), nil
        case string:
            return castedVal, nil
        default:
            return nil, fmt.Errorf("expected time.Time type, received: %T", value)
        }
    case schema.TimeNano:
        switch castedVal := value.(type) {
        case time.Time:
            return castedVal.Format(TimeNano), nil
        case string:
            return castedVal, nil
        default:
            return nil, fmt.Errorf("expected time.Time type, received: %T", value)
        }
    case schema.Datetime2:
        switch castedVal := value.(type) {
        case time.Time:
            return castedVal.Format(time.DateTime), nil
        case string:
            return castedVal, nil
        default:
            return nil, fmt.Errorf("expected time.Time type, received: %T", value)
        }
    case schema.Datetime2Micro:
        switch castedVal := value.(type) {
        case time.Time:
            return castedVal.Format(DateTimeMicro), nil
        case string:
            return castedVal, nil
        default:
            return nil, fmt.Errorf("expected time.Time type, received: %T", value)
        }
    case schema.Datetime2Nano:
        switch castedVal := value.(type) {
        case time.Time:
            return castedVal.Format(DateTimeNano), nil
        case string:
            return castedVal, nil
        default:
            return nil, fmt.Errorf("expected time.Time type, received: %T", value)
        }
    case schema.DatetimeOffset:
        switch castedVal := value.(type) {
        case time.Time:
            return castedVal.Format(DateTimeOffset), nil
        case string:
            return castedVal, nil
        default:
            return nil, fmt.Errorf("expected time.Time type, received: %T", value)
        }
    default:
        return value, nil
    }
}

func (s scanAdapter) BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any, error) {
    mssqlDialect := dialect.MSSQLDialect{}
    colNames := make([]string, len(s.columns))
    for idx, col := range s.columns {
        colNames[idx] = mssqlDialect.QuoteIdentifier(col.Name)
    }

    startingValues := make([]any, len(primaryKeys))
    endingValues := make([]any, len(primaryKeys))
    for i, pk := range primaryKeys {
        pkStartVal, err := s.parsePrimaryKeyValues(pk.Name, pk.StartingValue)
        if err != nil {
            return "", nil, fmt.Errorf("failed to parse start primary key val: %w", err)
        }

        pkEndVal, err := s.parsePrimaryKeyValues(pk.Name, pk.EndingValue)
        if err != nil {
            return "", nil, fmt.Errorf("failed to parse end primary key val: %w", err)
        }

        startingValues[i] = pkStartVal
        endingValues[i] = pkEndVal
    }

    quotedKeyNames := make([]string, len(primaryKeys))
    for i, key := range primaryKeys {
        quotedKeyNames[i] = mssqlDialect.QuoteIdentifier(key.Name)
    }

    lowerBoundComparison := ">"
    if isFirstBatch {
        lowerBoundComparison = ">="
    }

    return fmt.Sprintf(`SELECT TOP %d %s FROM %s.%s WHERE (%s) %s (%s) AND (%s) <= (%s) ORDER BY %s`,
        // TOP
        batchSize,
        // SELECT
        strings.Join(colNames, ","),
        // FROM
        mssqlDialect.QuoteIdentifier(s.schema), mssqlDialect.QuoteIdentifier(s.tableName),
        // WHERE (pk) > (123)
        strings.Join(quotedKeyNames, ","), lowerBoundComparison, strings.Join(rdbms.QueryPlaceholders("?", len(startingValues)), ","),
        strings.Join(quotedKeyNames, ","), strings.Join(rdbms.QueryPlaceholders("?", len(endingValues)), ","),
        // ORDER BY
        strings.Join(quotedKeyNames, ","),
    ), slices.Concat(startingValues, endingValues), nil
}

func (s scanAdapter) ParseRow(values []any) error {
    for i, value := range values {
        parsedValue, err := parse.ParseValue(s.columns[i].Type, value)
        if err != nil {
            return fmt.Errorf("failed to parse column: %q: %w", s.columns[i].Name, err)
        }

        values[i] = parsedValue
    }

    return nil
}
