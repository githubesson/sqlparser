package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"sqlparser/pkg/models"
	"sqlparser/pkg/parser"
	"sqlparser/pkg/writer"
)

func main() {
	format := flag.String("format", "txt", "Output format (txt, csv, json, jsonl)")
	output := flag.String("output", "", "Output file (if not specified, prints to stdout)")
	workers := flag.Int("workers", getWorkerCount(), "Number of worker threads")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Printf("Usage: sqlparser [-format=txt|csv|json] [-output=filename] [-workers=N] <sqlfile>\n")
		fmt.Printf("  -format: Output format (default: txt)\n")
		fmt.Printf("  -output: Output file (default: stdout)\n")
		fmt.Printf("  -workers: Number of worker threads (default: %d)\n", getWorkerCount())
		os.Exit(1)
	}

	filename := args[0]

	// Scan for tables and prompt for selection
	tables, err := parser.ScanTables(filename)
	if err != nil {
		fmt.Printf("Error scanning tables: %v\n", err)
		os.Exit(1)
	}

	selectedTable, err := parser.PromptTableSelection(tables)
	if err != nil {
		fmt.Printf("Error selecting table: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nSelected table: %s\n", selectedTable.Name)

	outputFormat := models.OutputFormat(*format)

	// Create output file or use stdout
	var out *os.File
	if *output != "" {
		out, err = os.Create(*output)
		if err != nil {
			fmt.Printf("Error creating output file: %v\n", err)
			os.Exit(1)
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	// Initialize the output writer based on format
	w, err := writer.CreateWriter(outputFormat, out)
	if err != nil {
		fmt.Printf("Error creating writer: %v\n", err)
		os.Exit(1)
	}
	defer w.Close()

	// Process the file
	fmt.Printf("Processing with %d workers...\n", *workers)
	err = parser.ProcessSQLFileInBatches(filename, w, *workers, selectedTable)
	if err != nil {
		fmt.Printf("Error processing SQL file: %v\n", err)
		os.Exit(1)
	}
}

func getWorkerCount() int {
	if val := os.Getenv("WORKER_COUNT"); val != "" {
		if count, err := strconv.Atoi(val); err == nil && count > 0 {
			return count
		}
	}
	return 1
}
