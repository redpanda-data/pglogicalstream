package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/usedatabrew/pglogicalstream/internal/schemas"
	"strings"
)

type ChangeFilter struct {
	tablesWhiteList map[string]*arrow.Schema
	schemaWhiteList string
}

type Filtered func(change []byte)

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

func (c ChangeFilter) FilterChange(change []byte, OnFiltered Filtered) {
	var changes WallMessage
	if err := json.NewDecoder(bytes.NewReader(change)).Decode(&changes); err != nil {
		panic(fmt.Errorf("cant parse change from database to filter it %v", err))
	}

	for _, ch := range changes.Change {
		var filteredChanges = Wal2JsonChanges{Changes: []Wal2JsonChange{}}
		if ch.Schema != c.schemaWhiteList {
			continue
		}

		var (
			arrowTableSchema *arrow.Schema
			tableExist       bool
		)

		if arrowTableSchema, tableExist = c.tablesWhiteList[ch.Table]; !tableExist {
			continue
		}

		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowTableSchema)
		changesMap := map[string]interface{}{}
		for i, changedValue := range ch.Columnvalues {
			changesMap[ch.Columnnames[i]] = changedValue
		}

		arrowSchema := c.tablesWhiteList[ch.Table]
		for i, _ := range ch.Columnnames {
			field := arrowSchema.Field(i)
			value := changesMap[field.Name]
			s := scalar.NewScalar(arrowSchema.Field(i).Type)
			if err := s.Set(value); err != nil {
				panic(fmt.Errorf("error setting value for column %s: %w", field.Name, err))
			}

			scalar.AppendToBuilder(builder.Field(i), s)
		}

		filteredChanges.Changes = append(filteredChanges.Changes, Wal2JsonChange{
			Kind:   ch.Kind,
			Schema: ch.Schema,
			Table:  ch.Table,
			Row:    builder.NewRecord(),
		})

		result, err := json.Marshal(&filteredChanges)
		if err != nil {
			panic(fmt.Errorf("cant marshal change after filtering %v", err))
		}
		OnFiltered(result)
	}
}
