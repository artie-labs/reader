package mssql

import (
    "github.com/artie-labs/reader/lib/mssql/schema"
    "github.com/stretchr/testify/assert"
    "testing"
    "time"
)

func TestScanAdapter_EncodePrimaryKeyValue(t *testing.T) {
    {
        // schema.Time
        adapter := scanAdapter{
            columns: []schema.Column{
                {
                    Name: "time",
                    Type: schema.Time,
                },
            },
        }

        // Able to use string
        val, err := adapter.encodePrimaryKeyValue("time", "12:34:56")
        assert.NoError(t, err)
        assert.Equal(t, "12:34:56", val)

        // Able to use time.Time
        td := time.Date(2021, 1, 1, 12, 34, 56, 0, time.UTC)
        val, err = adapter.encodePrimaryKeyValue("time", td)
        assert.NoError(t, err)
        assert.Equal(t, "12:34:56", val)
    }
    {
        // Schema.TimeMicro
        adapter := scanAdapter{
            columns: []schema.Column{
                {
                    Name: "time_micro",
                    Type: schema.TimeMicro,
                },
            },
        }

        // Able to use string
        val, err := adapter.encodePrimaryKeyValue("time_micro", "12:34:56.789012")
        assert.NoError(t, err)
        assert.Equal(t, "12:34:56.789012", val)

        // Able to use time.Time
        td := time.Date(2021, 1, 1, 12, 34, 56, 789012000, time.UTC)
        val, err = adapter.encodePrimaryKeyValue("time_micro", td)
        assert.NoError(t, err)
        assert.Equal(t, "12:34:56.789012", val)
    }
    {
        // schema.TimeNano
        adapter := scanAdapter{
            columns: []schema.Column{
                {
                    Name: "time_nano",
                    Type: schema.TimeNano,
                },
            },
        }

        // Able to use string
        val, err := adapter.encodePrimaryKeyValue("time_nano", "12:34:56.789012345")
        assert.NoError(t, err)
        assert.Equal(t, "12:34:56.789012345", val)

        // Able to use time.Time
        td := time.Date(2021, 1, 1, 12, 34, 56, 789012345, time.UTC)
        val, err = adapter.encodePrimaryKeyValue("time_nano", td)
        assert.NoError(t, err)
        assert.Equal(t, "12:34:56.789012345", val)
    }
    {
        // schema.Datetime2
        adapter := scanAdapter{
            columns: []schema.Column{
                {
                    Name: "datetime2",
                    Type: schema.Datetime2,
                },
            },
        }

        // Able to use string
        val, err := adapter.encodePrimaryKeyValue("datetime2", "2021-01-01 12:34:56")
        assert.NoError(t, err)
        assert.Equal(t, "2021-01-01 12:34:56", val)

        // Able to use time.Time
        td := time.Date(2021, 1, 1, 12, 34, 56, 0, time.UTC)
        val, err = adapter.encodePrimaryKeyValue("datetime2", td)
        assert.NoError(t, err)
        assert.Equal(t, "2021-01-01 12:34:56", val)
    }
    {
        // schema.Datetime2Micro
        adapter := scanAdapter{
            columns: []schema.Column{
                {
                    Name: "datetime2_micro",
                    Type: schema.Datetime2Micro,
                },
            },
        }

        // Able to use string
        val, err := adapter.encodePrimaryKeyValue("datetime2_micro", "2021-01-01 12:34:56.789012")
        assert.NoError(t, err)
        assert.Equal(t, "2021-01-01 12:34:56.789012", val)

        // Able to use time.Time
        td := time.Date(2021, 1, 1, 12, 34, 56, 789012000, time.UTC)
        val, err = adapter.encodePrimaryKeyValue("datetime2_micro", td)
        assert.NoError(t, err)
        assert.Equal(t, "2021-01-01 12:34:56.789012", val)
    }
    {
        // schema.Datetime2Nano
        adapter := scanAdapter{
            columns: []schema.Column{
                {
                    Name: "datetime2_nano",
                    Type: schema.Datetime2Nano,
                },
            },
        }

        // Able to use string
        val, err := adapter.encodePrimaryKeyValue("datetime2_nano", "2021-01-01 12:34:56.789012345")
        assert.NoError(t, err)
        assert.Equal(t, "2021-01-01 12:34:56.789012345", val)

        // Able to use time.Time
        td := time.Date(2021, 1, 1, 12, 34, 56, 789012345, time.UTC)
        val, err = adapter.encodePrimaryKeyValue("datetime2_nano", td)
        assert.NoError(t, err)
        assert.Equal(t, "2021-01-01 12:34:56.789012345", val)
    }
    {
        // schema.DatetimeOffset
        adapter := scanAdapter{
            columns: []schema.Column{
                {
                    Name: "datetimeoffset",
                    Type: schema.DatetimeOffset,
                },
            },
        }

        // Able to use string
        val, err := adapter.encodePrimaryKeyValue("datetimeoffset", "2021-01-01 12:34:56.7890123 +00:00")
        assert.NoError(t, err)
        assert.Equal(t, "2021-01-01 12:34:56.7890123 +00:00", val)

        // Able to use time.Time
        td := time.Date(2021, 1, 1, 12, 34, 56, 789012300, time.UTC)
        val, err = adapter.encodePrimaryKeyValue("datetimeoffset", td)
        assert.NoError(t, err)
        assert.Equal(t, "2021-01-01 12:34:56.7890123 +00:00", val)
    }
}
