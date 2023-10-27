package schemas

import "github.com/apache/arrow/go/v14/arrow"

type DataTableSchema struct {
	TableName string
	Schema    *arrow.Schema
}
