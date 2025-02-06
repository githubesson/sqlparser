package parser

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"sqlparser/pkg/models"
	"sqlparser/pkg/writer"
)

func ProcessSQLFileInBatches(filename string, writer writer.Writer, numWorkers int) error {
	startTime := time.Now()

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Use a larger buffer for scanning
	buf := make([]byte, 64*1024)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(buf, 10*1024*1024) // 10MB max line length

	statementChan := make(chan string, numWorkers*2)
	resultChan := make(chan *struct {
		tableName string
		rows      []models.Row
		err       error
	}, numWorkers*2)

	fmt.Printf("Starting to process file: %s at %s\n", filename, startTime.Format(time.RFC3339))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for statement := range statementChan {
				tableName, rows, err := processStatement(statement, numWorkers)
				resultChan <- &struct {
					tableName string
					rows      []models.Row
					err       error
				}{tableName, rows, err}
			}
		}(i)
	}

	// Start result processor
	var processWg sync.WaitGroup
	processWg.Add(1)
	var currentTableName string
	var rowCount int
	var currentBatch []models.Row
	var batchCount int
	var totalStatements int
	var tableStartTime time.Time

	go func() {
		defer processWg.Done()
		for result := range resultChan {
			if result.err != nil {
				fmt.Printf("Error processing statement: %v\n", result.err)
				continue
			}

			if result.rows != nil {
				// Handle new table
				if result.tableName != currentTableName {
					if currentTableName != "" {
						if len(currentBatch) > 0 {
							if err := writer.WriteRows(currentBatch); err != nil {
								fmt.Printf("Error writing rows: %v\n", err)
								continue
							}
							batchCount++
							fmt.Printf("Processed batch %d for table %s (%d rows)\n", batchCount, currentTableName, len(currentBatch))
							currentBatch = nil // Help GC
						}
						if err := writer.WriteTableEnd(); err != nil {
							fmt.Printf("Error ending table: %v\n", err)
							continue
						}
						tableDuration := time.Since(tableStartTime)
						fmt.Printf("Finished processing table %s (total %d rows in %d batches) in %v\n",
							currentTableName, rowCount, batchCount, tableDuration)
					}
					currentTableName = result.tableName
					if err := writer.WriteTableStart(currentTableName); err != nil {
						fmt.Printf("Error starting table: %v\n", err)
						continue
					}
					currentBatch = make([]models.Row, 0, models.BatchSize)
					rowCount = 0
					batchCount = 0
					tableStartTime = time.Now()
					fmt.Printf("Started processing new table: %s at %s\n", currentTableName, tableStartTime.Format(time.RFC3339))
				}

				// Process rows immediately
				for _, row := range result.rows {
					rowCount++
					row.RowNumber = rowCount
					currentBatch = append(currentBatch, row)

					// Write batch if it reaches the batch size
					if len(currentBatch) >= models.BatchSize {
						batchStartTime := time.Now()
						if err := writer.WriteRows(currentBatch); err != nil {
							fmt.Printf("Error writing rows: %v\n", err)
							continue
						}
						batchCount++
						batchDuration := time.Since(batchStartTime)
						fmt.Printf("Processed batch %d for table %s (%d rows) in %v\n",
							batchCount, currentTableName, len(currentBatch), batchDuration)
						currentBatch = make([]models.Row, 0, models.BatchSize)
					}
				}
				// Clear the rows to help GC
				result.rows = nil
			}
			totalStatements++
		}
	}()

	// Read and send statements to workers
	var currentStatement strings.Builder
	for scanner.Scan() {
		line := scanner.Text()

		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}

		currentStatement.WriteString(line)
		currentStatement.WriteString(" ")

		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			statementChan <- currentStatement.String()
			currentStatement.Reset()
		}
	}

	// Close channels and wait for completion
	close(statementChan)
	wg.Wait()
	close(resultChan)
	processWg.Wait()

	// Write any remaining rows in the last batch
	if len(currentBatch) > 0 {
		if err := writer.WriteRows(currentBatch); err != nil {
			return err
		}
		batchCount++
		fmt.Printf("Processed final batch %d for table %s (%d rows)\n", batchCount, currentTableName, len(currentBatch))
		currentBatch = nil // Help GC
	}

	// Close the last table if any
	if currentTableName != "" {
		if err := writer.WriteTableEnd(); err != nil {
			return err
		}
		tableDuration := time.Since(tableStartTime)
		fmt.Printf("Finished processing table %s (total %d rows in %d batches) in %v\n",
			currentTableName, rowCount, batchCount, tableDuration)
	}

	totalDuration := time.Since(startTime)
	fmt.Printf("\nProcessing Summary:\n")
	fmt.Printf("File: %s\n", filename)
	fmt.Printf("Total Statements: %d\n", totalStatements)
	fmt.Printf("Total Duration: %v\n", totalDuration)
	fmt.Printf("Average Time per Statement: %v\n", totalDuration/time.Duration(totalStatements))
	fmt.Printf("Workers Used: %d\n", numWorkers)

	return scanner.Err()
}

func processStatement(statement string, numWorkers int) (string, []models.Row, error) {
	statement = strings.TrimSpace(strings.TrimSuffix(statement, ";"))

	if strings.HasPrefix(strings.ToUpper(statement), "INSERT INTO") {
		return parseInsert(statement, numWorkers)
	}
	return "", nil, nil
}

func parseInsert(statement string, numWorkers int) (string, []models.Row, error) {
	parts := strings.SplitN(statement, "VALUES", 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid INSERT statement format")
	}

	insertPart := strings.TrimSpace(parts[0])
	if !strings.HasPrefix(strings.ToUpper(insertPart), "INSERT INTO") {
		return "", nil, fmt.Errorf("invalid INSERT statement format")
	}

	tableParts := strings.SplitN(insertPart[11:], "(", 2)
	if len(tableParts) != 2 {
		return "", nil, fmt.Errorf("invalid INSERT statement format")
	}

	tableName := strings.Trim(strings.TrimSpace(tableParts[0]), "`")
	columnsPart := strings.TrimRight(tableParts[1], ")")
	columns := parseColumnList(columnsPart)

	valuesPart := strings.TrimSpace(parts[1])
	values := parseValuesList(valuesPart)

	// Process rows in parallel if we have enough rows
	if len(values) > 1000 {
		return parseRowsParallel(tableName, columns, values, numWorkers)
	}

	return parseRowsSequential(tableName, columns, values)
}

func parseColumnList(columnsPart string) []string {
	columns := strings.Split(columnsPart, ",")
	result := make([]string, 0, len(columns))

	for _, col := range columns {
		col = strings.Trim(strings.TrimSpace(col), "`")
		if col != "" {
			result = append(result, col)
		}
	}

	return result
}
