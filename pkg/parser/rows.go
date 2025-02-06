package parser

import (
	"sync"

	"sqlparser/pkg/models"
)

// Pre-allocate maps for row data to reduce GC pressure
var rowDataPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 50) // typical column count
	},
}

func parseRowsSequential(tableName string, columns []string, values [][]string) (string, []models.Row, error) {
	rows := make([]models.Row, len(values))
	for i, rowValues := range values {
		rowData := rowDataPool.Get().(map[string]interface{})
		for j, value := range rowValues {
			if j < len(columns) {
				if value == "NULL" {
					rowData[columns[j]] = nil
				} else if value == "" {
					rowData[columns[j]] = ""
				} else {
					rowData[columns[j]] = value
				}
			}
		}
		rows[i] = models.Row{
			Data: rowData,
		}
	}
	return tableName, rows, nil
}

func parseRowsParallel(tableName string, columns []string, values [][]string, numWorkers int) (string, []models.Row, error) {
	rowsPerWorker := (len(values) + numWorkers - 1) / numWorkers

	rows := make([]models.Row, len(values))
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * rowsPerWorker
		if start >= len(values) {
			break
		}

		end := start + rowsPerWorker
		if end > len(values) {
			end = len(values)
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for idx := start; idx < end; idx++ {
				rowData := rowDataPool.Get().(map[string]interface{})
				for j, value := range values[idx] {
					if j < len(columns) {
						if value == "NULL" {
							rowData[columns[j]] = nil
						} else if value == "" {
							rowData[columns[j]] = ""
						} else {
							rowData[columns[j]] = value
						}
					}
				}
				rows[idx] = models.Row{
					Data: rowData,
				}
			}
		}(start, end)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Check for errors
	select {
	case err := <-errors:
		return "", nil, err
	default:
		return tableName, rows, nil
	}
}
