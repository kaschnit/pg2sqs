package replication

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

func decodeTuple(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	if tuple == nil {
		return nil
	}

	data := make(map[string]any, len(tuple.Columns))
	for idx, col := range tuple.Columns {
		name := fmt.Sprintf("col%d", idx)
		if idx < len(rel.Columns) {
			name = rel.Columns[idx].Name
		}

		data[name] = decodeTupleColumn(col)
	}

	return data
}

func decodeTupleColumn(column *pglogrepl.TupleDataColumn) any {
	switch column.DataType {
	case pglogrepl.TupleDataTypeNull:
		return nil
	case pglogrepl.TupleDataTypeText:
		return string(column.Data)
	case pglogrepl.TupleDataTypeBinary:
		return column.Data
	case pglogrepl.TupleDataTypeToast:
		return nil
	default:
		return string(column.Data)
	}
}
