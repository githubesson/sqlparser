package writer

import (
	"bufio"
	"encoding/csv"
	"fmt"

	"sqlparser/pkg/models"
)

type CSVWriter struct {
	writer    *csv.Writer
	buffer    *bufio.Writer
	columns   []string
	tableName string
}

func NewCSVWriter(output *bufio.Writer) *CSVWriter {
	return &CSVWriter{
		writer: csv.NewWriter(output),
		buffer: output,
	}
}

func (w *CSVWriter) WriteTableStart(tableName string) error {
	w.tableName = tableName
	return w.writer.Write([]string{"Table:", tableName})
}

func (w *CSVWriter) WriteRows(rows []models.Row) error {
	if len(rows) == 0 {
		return nil
	}

	// Write headers if this is the first batch
	if w.columns == nil {
		w.columns = make([]string, 0, len(rows[0].Data))
		for col := range rows[0].Data {
			w.columns = append(w.columns, col)
		}
		if err := w.writer.Write(append([]string{"Row"}, w.columns...)); err != nil {
			return err
		}
	}

	// Write data rows
	for _, row := range rows {
		rowData := make([]string, 0, len(w.columns)+1)
		rowData = append(rowData, fmt.Sprintf("%d", row.RowNumber))
		for _, col := range w.columns {
			val := row.Data[col]
			if val == nil {
				rowData = append(rowData, "NULL")
			} else {
				rowData = append(rowData, fmt.Sprintf("%v", val))
			}
		}
		if err := w.writer.Write(rowData); err != nil {
			return err
		}
	}
	w.writer.Flush()
	return nil
}

func (w *CSVWriter) WriteTableEnd() error {
	w.columns = nil
	if err := w.writer.Write([]string{}); err != nil {
		return err
	}
	w.writer.Flush()
	return nil
}

func (w *CSVWriter) Close() error {
	w.writer.Flush()
	return w.buffer.Flush()
}
