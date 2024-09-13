package pglogicalstream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/usedatabrew/pglogicalstream/internal/schemas"
)

type ChangeFilter struct {
	tablesWhiteList map[string]*arrow.Schema
	schemaWhiteList string
}

type Filtered func(change Wal2JsonChanges)

func NewChangeFilter(tableSchemas []schemas.DataTableSchema, schema string) ChangeFilter {
	tablesMap := map[string]*arrow.Schema{}
	for _, table := range tableSchemas {
		tablesMap[strings.Split(table.TableName, ".")[1]] = table.Schema
	}

	return ChangeFilter{
		tablesWhiteList: tablesMap,
		schemaWhiteList: schema,
	}
}

func (c ChangeFilter) FilterChange(lsn string, change []byte, OnFiltered Filtered) {
	var changes WallMessage
	if err := json.NewDecoder(bytes.NewReader(change)).Decode(&changes); err != nil {
		panic(fmt.Errorf("cant parse change from database to filter it %v", err))
	}

	if len(changes.Change) == 0 {
		return
	}

	for _, ch := range changes.Change {
		var filteredChanges = Wal2JsonChanges{
			Lsn:     lsn,
			Changes: []Wal2JsonChange{},
		}
		if ch.Schema != c.schemaWhiteList {
			continue
		}

		if _, tableExist := c.tablesWhiteList[ch.Table]; !tableExist {
			continue
		}

		filteredChanges.Changes = append(filteredChanges.Changes, Wal2JsonChange{
			Kind:   ch.Kind,
			Schema: ch.Schema,
			Table:  ch.Table,
			Row:    ch,
		})

		OnFiltered(filteredChanges)
	}
}
