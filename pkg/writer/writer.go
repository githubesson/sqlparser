package writer

import (
	"bufio"
	"fmt"
	"io"

	"sqlparser/pkg/models"
)

const bufferSize = 256 * 1024 // 256KB buffer

type Writer interface {
	WriteTableStart(tableName string) error
	WriteRows(rows []models.Row) error
	WriteTableEnd() error
	Close() error
	Type() models.OutputFormat
}

func CreateWriter(format models.OutputFormat, output io.Writer) (Writer, error) {
	bufferedWriter := bufio.NewWriterSize(output, bufferSize)

	switch format {
	case models.FormatJSON:
		return NewJSONWriter(bufferedWriter)
	case models.FormatJSONL:
		return NewJSONLWriter(bufferedWriter)
	case models.FormatCSV:
		return NewCSVWriter(bufferedWriter), nil
	case models.FormatText:
		return NewTextWriter(bufferedWriter), nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}
