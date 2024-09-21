package pglogicalstream

type Wal2JsonChanges struct {
	Lsn     *string          `json:"lsn"`
	Changes []Wal2JsonChange `json:"change"`
}

type Wal2JsonChange struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnTypes  []string      `json:"columntypes"`
	ColumnValues []interface{} `json:"columnvalues"`
}
type OnMessage = func(message Wal2JsonChanges)
