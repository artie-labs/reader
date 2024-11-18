package streaming

import (
	"database/sql"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/maputil"
	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/sources/mysql/adapter"
	"time"
)

type SchemaHistoryAdapter struct {
	cfg           config.MySQL
	schemaHistory map[string]*maputil.MostRecentMap[adapter.Table]
}

func (s SchemaHistoryAdapter) connect() (*sql.DB, error) {
	return sql.Open("mysql", s.cfg.ToDSN())
}

func BuildTablesAdapter(cfg config.MySQL) (map[string]*maputil.MostRecentMap[adapter.Table], error) {
	db, err := sql.Open("mysql", cfg.ToDSN())
	if err != nil {
		return nil, err
	}

	retMap := make(map[string]*maputil.MostRecentMap[adapter.Table])
	for _, tableCfg := range cfg.Tables {
		tbl, err := mysql.LoadTable(db, tableCfg.Name)
		if err != nil {
			return nil, err
		}

		tableAdapter, err := adapter.BuildTableAdapter(*tableCfg, *tbl)
		if err != nil {
			return nil, err
		}

		val := maputil.NewMostRecentMap[adapter.Table]()
		val.AddItem(time.Now().UnixMilli(), tableAdapter)
		retMap[tableCfg.Name] = val
	}

	return retMap, nil
}
