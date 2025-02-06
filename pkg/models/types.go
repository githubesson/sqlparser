package models

const (
	BatchSize = 100000
)

type OutputFormat string

const (
	FormatText OutputFormat = "txt"
	FormatCSV  OutputFormat = "csv"
	FormatJSON OutputFormat = "json"
)

type Row struct {
	RowNumber int                    `json:"row_number"`
	Data      map[string]interface{} `json:"data"`
}

type TableData struct {
	TableName string `json:"table_name"`
	RowCount  int    `json:"row_count"`
	Rows      []Row  `json:"rows"`
}
