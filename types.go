package pglogicalstream

import (
	"github.com/apache/arrow/go/v14/arrow"
)

type Wal2JsonChanges struct {
	Lsn     string
	Changes []Wal2JsonChange `json:"change"`
}

type Wal2JsonChange struct {
	Kind   string       `json:"action"`
	Schema string       `json:"schema"`
	Table  string       `json:"table"`
	Row    arrow.Record `json:"data"`
}

type OnMessage = func(message Wal2JsonChanges)
