package helpers

import "github.com/apache/arrow/go/v14/arrow"

func MapPlainTypeToArrow(fieldType string) arrow.DataType {
	switch fieldType {
	case "Boolean":
		return arrow.FixedWidthTypes.Boolean
	case "Int16":
		return arrow.PrimitiveTypes.Int16
	case "Int32":
		return arrow.PrimitiveTypes.Int32
	case "Int64":
		return arrow.PrimitiveTypes.Int64
	case "Uint64":
		return arrow.PrimitiveTypes.Uint64
	case "Float64":
		return arrow.PrimitiveTypes.Float64
	case "Float32":
		return arrow.PrimitiveTypes.Float32
	case "UUID":
		return arrow.BinaryTypes.String
	case "bytea":
		return arrow.BinaryTypes.Binary
	case "JSON":
		return arrow.BinaryTypes.String
	case "Inet":
		return arrow.BinaryTypes.String
	case "MAC":
		return arrow.BinaryTypes.String
	case "Date32":
		return arrow.FixedWidthTypes.Date32
	default:
		return arrow.BinaryTypes.String
	}
}
