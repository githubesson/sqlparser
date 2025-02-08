package models

import (
	"os"
	"strconv"
)

var (
	BatchSize = getBatchSize()
)

func getBatchSize() int {
	if val := os.Getenv("BATCH_SIZE"); val != "" {
		if size, err := strconv.Atoi(val); err == nil && size > 0 {
			return size
		}
	}
	return 100000 // default batch size
}

type OutputFormat string

const (
	FormatText  OutputFormat = "txt"
	FormatCSV   OutputFormat = "csv"
	FormatJSON  OutputFormat = "json"
	FormatJSONL OutputFormat = "jsonl"
)

type Row struct {
	TableName string                 `json:"table_name,omitempty"`
	RowNumber int                    `json:"row_number"`
	Data      map[string]interface{} `json:"data"`
}

type TableData struct {
	TableName string `json:"table_name"`
	RowCount  int    `json:"row_count"`
	Rows      []Row  `json:"rows"`
}
