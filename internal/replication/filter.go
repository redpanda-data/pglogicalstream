package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type ChangeFilter struct {
	tablesWhiteList map[string]string
	schemaWhiteList string
}

func NewChangeFilter(tables []string, schema string) ChangeFilter {
	tablesMap := map[string]string{}
	for _, table := range tables {
		tablesMap[table] = table
	}

	return ChangeFilter{
		tablesWhiteList: tablesMap,
		schemaWhiteList: schema,
	}
}

func (c ChangeFilter) FilterChange(change []byte) []byte {
	var changes Wal2JsonChanges
	if err := json.NewDecoder(bytes.NewReader(change)).Decode(&changes); err != nil {
		panic(fmt.Errorf("cant parse change from database to filter it %v", err))
	}

	var filteredChanges = Wal2JsonChanges{Changes: []Wal2JsonChange{}}
	for _, ch := range changes.Changes {
		if ch.Schema != c.schemaWhiteList {
			continue
		}
		if _, ok := c.tablesWhiteList[ch.Table]; !ok {
			continue
		}
		filteredChanges.Changes = append(filteredChanges.Changes, ch)
	}

	result, err := json.Marshal(&filteredChanges)
	if err != nil {
		panic(fmt.Errorf("cant marshal change after filtering %v", err))
	}

	return result
}
