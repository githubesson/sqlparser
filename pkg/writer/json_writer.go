package writer

import (
	"bufio"
	"encoding/json"
	"fmt"

	"sqlparser/pkg/models"
)

type JSONWriter struct {
	writer      *bufio.Writer
	firstTable  bool
	firstBatch  bool
	tableOpened bool
}

func NewJSONWriter(output *bufio.Writer) (*JSONWriter, error) {
	if _, err := output.Write([]byte("[")); err != nil {
		return nil, err
	}
	if err := output.Flush(); err != nil {
		return nil, err
	}
	return &JSONWriter{writer: output, firstTable: true}, nil
}

func (w *JSONWriter) WriteTableStart(tableName string) error {
	if !w.firstTable {
		if _, err := w.writer.Write([]byte(",\n")); err != nil {
			return err
		}
	}
	w.firstTable = false
	w.firstBatch = true
	w.tableOpened = true
	_, err := fmt.Fprintf(w.writer, `{"table_name":"%s","rows":[`, tableName)
	return err
}

func (w *JSONWriter) WriteRows(rows []models.Row) error {
	if !w.firstBatch {
		if _, err := w.writer.Write([]byte(",")); err != nil {
			return err
		}
	}
	w.firstBatch = false

	for i, row := range rows {
		if i > 0 {
			if _, err := w.writer.Write([]byte(",")); err != nil {
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

func (w *JSONWriter) WriteTableEnd() error {
	if w.tableOpened {
		if _, err := w.writer.Write([]byte("]}")); err != nil {
			return err
		}
		w.tableOpened = false
		return w.writer.Flush()
	}
	return nil
}

func (w *JSONWriter) Close() error {
	if w.tableOpened {
		if err := w.WriteTableEnd(); err != nil {
			return err
		}
	}
	if _, err := w.writer.Write([]byte("\n")); err != nil {
		return err
	}
	return w.writer.Flush()
}
