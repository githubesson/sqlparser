package parser

import (
	"runtime"
	"strings"
	"sync"
)

func parseValuesList(valuesPart string) [][]string {
	// Pre-allocate capacity based on rough estimate
	estimatedRows := strings.Count(valuesPart, "),(") + 1
	values := make([][]string, 0, estimatedRows)

	// Use a pool of builders to reduce allocations
	builderPool := sync.Pool{
		New: func() interface{} {
			builder := &strings.Builder{}
			builder.Grow(100) // typical field size
			return builder
		},
	}

	// Process in chunks for large inputs
	const chunkSize = 1024 * 1024 // 1MB chunks
	if len(valuesPart) > chunkSize*2 {
		return parseValuesListParallel(valuesPart, &builderPool)
	}

	var currentValue []string
	fieldBuf := builderPool.Get().(*strings.Builder)
	defer builderPool.Put(fieldBuf)

	inQuotes := false
	inParentheses := 0

	// Process the string in a single pass
	for i := 0; i < len(valuesPart); i++ {
		char := valuesPart[i]

		switch char {
		case '(':
			if !inQuotes {
				inParentheses++
				if inParentheses == 1 {
					currentValue = make([]string, 0, 10)
					continue
				}
			}
		case ')':
			if !inQuotes {
				inParentheses--
				if inParentheses == 0 {
					if fieldBuf.Len() > 0 {
						currentValue = append(currentValue, strings.TrimSpace(fieldBuf.String()))
						fieldBuf.Reset()
					}
					values = append(values, currentValue)
					continue
				}
			}
		case '\'':
			if i > 0 && valuesPart[i-1] != '\\' {
				inQuotes = !inQuotes
				continue
			}
		case ',':
			if !inQuotes && inParentheses == 1 {
				currentValue = append(currentValue, strings.TrimSpace(fieldBuf.String()))
				fieldBuf.Reset()
				continue
			}
		}

		if inParentheses > 0 {
			fieldBuf.WriteByte(char)
		}
	}

	// Process the values in parallel if we have enough rows
	if len(values) > 1000 {
		processValuesParallel(values)
	} else {
		processValuesSequential(values)
	}

	return values
}

func parseValuesListParallel(valuesPart string, builderPool *sync.Pool) [][]string {
	numWorkers := runtime.NumCPU()
	chunkSize := (len(valuesPart) + numWorkers - 1) / numWorkers

	type chunkResult struct {
		values [][]string
		index  int
	}

	results := make(chan chunkResult, numWorkers)
	var wg sync.WaitGroup

	// Process chunks in parallel
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(valuesPart) {
			break
		}

		end := start + chunkSize
		if end > len(valuesPart) {
			end = len(valuesPart)
		}

		// Adjust chunk boundaries to not split in the middle of a value
		if start > 0 {
			for start < len(valuesPart) && valuesPart[start] != '(' {
				start++
			}
		}
		if end < len(valuesPart) {
			for end > start && valuesPart[end] != ')' {
				end--
			}
			end++ // include the closing parenthesis
		}

		wg.Add(1)
		go func(start, end, index int) {
			defer wg.Done()

			fieldBuf := builderPool.Get().(*strings.Builder)
			defer builderPool.Put(fieldBuf)

			chunk := valuesPart[start:end]
			values := parseValuesChunk(chunk, fieldBuf)
			results <- chunkResult{values: values, index: index}
		}(start, end, i)
	}

	// Close results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and merge results
	var allValues [][]string
	for result := range results {
		allValues = append(allValues, result.values...)
	}

	return allValues
}

func parseValuesChunk(chunk string, fieldBuf *strings.Builder) [][]string {
	var values [][]string
	var currentValue []string
	inQuotes := false
	inParentheses := 0

	for i := 0; i < len(chunk); i++ {
		char := chunk[i]

		switch char {
		case '(':
			if !inQuotes {
				inParentheses++
				if inParentheses == 1 {
					currentValue = make([]string, 0, 10)
					continue
				}
			}
		case ')':
			if !inQuotes {
				inParentheses--
				if inParentheses == 0 {
					if fieldBuf.Len() > 0 {
						currentValue = append(currentValue, strings.TrimSpace(fieldBuf.String()))
						fieldBuf.Reset()
					}
					values = append(values, currentValue)
					continue
				}
			}
		case '\'':
			if i > 0 && chunk[i-1] != '\\' {
				inQuotes = !inQuotes
				continue
			}
		case ',':
			if !inQuotes && inParentheses == 1 {
				currentValue = append(currentValue, strings.TrimSpace(fieldBuf.String()))
				fieldBuf.Reset()
				continue
			}
		}

		if inParentheses > 0 {
			fieldBuf.WriteByte(char)
		}
	}

	return values
}

func processValuesSequential(values [][]string) {
	for i, row := range values {
		for j, val := range row {
			val = strings.TrimSpace(val)
			if strings.ToUpper(val) == "NULL" {
				values[i][j] = "NULL"
				continue
			}
			if val == "''" || val == "" {
				values[i][j] = ""
				continue
			}
			if strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'") {
				values[i][j] = val[1 : len(val)-1]
			}
		}
	}
}

func processValuesParallel(values [][]string) {
	numWorkers := runtime.NumCPU()
	rowsPerWorker := (len(values) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
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
				for j, val := range values[idx] {
					val = strings.TrimSpace(val)
					if strings.ToUpper(val) == "NULL" {
						values[idx][j] = "NULL"
						continue
					}
					if val == "''" || val == "" {
						values[idx][j] = ""
						continue
					}
					if strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'") {
						values[idx][j] = val[1 : len(val)-1]
					}
				}
			}
		}(start, end)
	}
	wg.Wait()
}
