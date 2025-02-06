package writer

import (
	"bufio"
	"fmt"

	"sqlparser/pkg/models"
)

type TextWriter struct {
	writer *bufio.Writer
}

func NewTextWriter(output *bufio.Writer) *TextWriter {
	return &TextWriter{writer: output}
}

func (w *TextWriter) WriteTableStart(tableName string) error {
	if _, err := fmt.Fprintf(w.writer, "\nTable: %s\n", tableName); err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *TextWriter) WriteRows(rows []models.Row) error {
	for _, row := range rows {
		if _, err := fmt.Fprintf(w.writer, "\nRow %d:\n", row.RowNumber); err != nil {
			return err
		}
		for col, val := range row.Data {
			if val == nil {
				if _, err := fmt.Fprintf(w.writer, "  %s: NULL\n", col); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(w.writer, "  %s: %v\n", col, val); err != nil {
					return err
				}
			}
		}
	}
	return w.writer.Flush()
}

func (w *TextWriter) WriteTableEnd() error {
	if _, err := w.writer.WriteString("\n"); err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *TextWriter) Close() error {
	return w.writer.Flush()
}
