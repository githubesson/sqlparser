package parser

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
)

type TableInfo struct {
	Name     string
	LineFrom int
	LineTo   int
}

func ScanTables(filename string) ([]TableInfo, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Use a larger buffer for scanning
	buf := make([]byte, 64*1024)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(buf, 10*1024*1024) // 10MB max line length

	// Use a map to track unique tables
	tableMap := make(map[string]*TableInfo)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		// Check for INSERT INTO statements
		if strings.HasPrefix(strings.ToUpper(line), "INSERT INTO") {
			parts := strings.Split(line, "`")
			if len(parts) >= 2 {
				tableName := parts[1]
				if _, exists := tableMap[tableName]; !exists {
					tableMap[tableName] = &TableInfo{
						Name:     tableName,
						LineFrom: lineNum,
					}
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning file: %v", err)
	}

	// Convert map to slice
	var tables []TableInfo
	for _, table := range tableMap {
		tables = append(tables, *table)
	}

	// Sort tables by name for consistent display
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})

	return tables, nil
}

func PromptTableSelection(tables []TableInfo) (*TableInfo, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables found in the file")
	}

	fmt.Println("\nFound the following tables with INSERT statements:")
	for i, table := range tables {
		fmt.Printf("%d. %s\n", i+1, table.Name)
	}

	var choice int
	fmt.Print("\nEnter the number of the table you want to parse (1-" + fmt.Sprint(len(tables)) + "): ")
	_, err := fmt.Scanf("%d", &choice)
	if err != nil || choice < 1 || choice > len(tables) {
		return nil, fmt.Errorf("invalid selection")
	}

	return &tables[choice-1], nil
}
