package ddl

import "time"

type SchemaHistory struct {
	tableHistory map[string][]TableDDL `yaml:"table_history"`
}

type TableDDL struct {
	ts  time.Time `yaml:"ts"`
	ddl string    `yaml:"ddl"`
}

func NewTableDDL(ts time.Time, ddl string) TableDDL {
	return TableDDL{
		ts:  ts,
		ddl: ddl,
	}
}

func NewSchemaHistory() SchemaHistory {
	return SchemaHistory{
		tableHistory: make(map[string][]TableDDL),
	}
}

func (s *SchemaHistory) AddTableDDL(tableName string, ddl TableDDL) {
	if _, ok := s.tableHistory[tableName]; !ok {
		s.tableHistory[tableName] = []TableDDL{}
	}

	s.tableHistory[tableName] = append(s.tableHistory[tableName], ddl)
}

func (s *SchemaHistory) GetTableDDLs(tableName string, highWaterMark time.Time) []TableDDL {
	if _, ok := s.tableHistory[tableName]; !ok {
		return []TableDDL{}
	}

	var tableDDLs []TableDDL
	for _, ddl := range s.tableHistory[tableName] {
		if highWaterMark.After(ddl.ts) {
			tableDDLs = append(tableDDLs, ddl)
		} else {
			break
		}
	}

	return tableDDLs
}
