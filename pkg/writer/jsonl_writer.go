package writer

import (
	"bufio"
	"encoding/json"

	"sqlparser/pkg/models"
)

type JSONLWriter struct {
	writer *bufio.Writer
}

func NewJSONLWriter(output *bufio.Writer) (*JSONLWriter, error) {
	return &JSONLWriter{writer: output}, nil
}

func (w *JSONLWriter) WriteTableStart(tableName string) error {
	return nil
}

func (w *JSONLWriter) WriteRows(rows []models.Row) error {
	if len(rows) == 0 {
		return nil
	}

	for i, row := range rows {
		if i > 0 {
			if _, err := w.writer.Write([]byte("\n")); err != nil {
				return err
			}
		}
		data, err := json.Marshal(row)
		if err != nil {
			return err
		}
		if _, err = w.writer.Write(data); err != nil {
			return err
		}
	}
	return w.writer.Flush()
}

func (w *JSONLWriter) WriteTableEnd() error {
	return nil
}

func (w *JSONLWriter) Close() error {
	return w.writer.Flush()
}

func (w *JSONLWriter) Type() models.OutputFormat {
	return models.FormatJSONL
}
