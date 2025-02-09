package writer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

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

type MultiWriter struct {
	format  models.OutputFormat
	writers map[string]Writer
	baseDir string
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

func CreateMultiWriter(format models.OutputFormat, inputPath string) (*MultiWriter, error) {
	// Use the input file name (without extension) as the base directory
	baseDir := filepath.Base(inputPath)
	if filepath.Ext(baseDir) != "" {
		baseDir = baseDir[:len(baseDir)-len(filepath.Ext(baseDir))]
	}

	// Create the directory
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	return &MultiWriter{
		format:  format,
		baseDir: baseDir,
		writers: make(map[string]Writer),
	}, nil
}

func (mw *MultiWriter) WriteTableStart(tableName string) error {
	// Create a new file for this table
	filename := filepath.Join(mw.baseDir, tableName+"."+mw.format.Extension())
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file for table %s: %v", tableName, err)
	}

	// Create a writer for this table
	writer, err := CreateWriter(mw.format, file)
	if err != nil {
		file.Close()
		return err
	}

	mw.writers[tableName] = writer
	return writer.WriteTableStart(tableName)
}

func (mw *MultiWriter) WriteRows(rows []models.Row) error {
	if len(rows) == 0 {
		return nil
	}

	// Get the writer for this table's rows
	tableName := rows[0].TableName
	writer, exists := mw.writers[tableName]
	if !exists {
		return fmt.Errorf("no writer found for table %s", tableName)
	}

	return writer.WriteRows(rows)
}

func (mw *MultiWriter) WriteTableEnd() error {
	var lastErr error
	for tableName, writer := range mw.writers {
		if err := writer.WriteTableEnd(); err != nil {
			lastErr = fmt.Errorf("failed to end table %s: %v", tableName, err)
		}
	}
	return lastErr
}

func (mw *MultiWriter) Close() error {
	var lastErr error
	for tableName, writer := range mw.writers {
		if err := writer.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close writer for table %s: %v", tableName, err)
		}
	}
	return lastErr
}

func (mw *MultiWriter) Type() models.OutputFormat {
	return mw.format
}
