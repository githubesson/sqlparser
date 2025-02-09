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
	format := flag.String("format", "", "Output format (txt, csv, json, jsonl)")
	output := flag.String("output", "", "Output file (for single table export)")
	workers := flag.Int("workers", getWorkerCount(), "Number of worker threads")
	exportAll := flag.Bool("all", false, "Export all tables (creates a directory named after the input file)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Printf("Usage: sqlparser [-format=txt|csv|json] [-output=filename] [-workers=N] [-all] <sqlfile>\n")
		fmt.Printf("  -format: Output format (txt, csv, json, jsonl). If specified without -output, creates files in a directory\n")
		fmt.Printf("  -output: Output file (optional, defaults to directory output if format is specified)\n")
		fmt.Printf("  -workers: Number of worker threads (default: %d)\n", getWorkerCount())
		fmt.Printf("  -all: Export all tables into separate files (default: false)\n")
		os.Exit(1)
	}

	filename := args[0]

	// Scan for tables
	tables, err := parser.ScanTables(filename)
	if err != nil {
		fmt.Printf("Error scanning tables: %v\n", err)
		os.Exit(1)
	}

	var selectedTables []*parser.TableInfo
	if *exportAll {
		selectedTables = make([]*parser.TableInfo, len(tables))
		for i := range tables {
			selectedTables[i] = &tables[i]
		}
		fmt.Printf("\nExporting all %d tables\n", len(tables))
	} else {
		selectedTable, err := parser.PromptTableSelection(tables)
		if err != nil {
			// Check if all tables were selected from the menu
			if allSelected, ok := err.(*parser.AllTablesSelected); ok {
				selectedTables = make([]*parser.TableInfo, len(allSelected.Tables))
				for i := range allSelected.Tables {
					selectedTables[i] = &allSelected.Tables[i]
				}
				fmt.Printf("\nExporting all %d tables\n", len(tables))
			} else {
				fmt.Printf("Error selecting table: %v\n", err)
				os.Exit(1)
			}
		} else {
			selectedTables = []*parser.TableInfo{selectedTable}
			fmt.Printf("\nSelected table: %s\n", selectedTable.Name)
		}
	}

	// If format is specified but no output, use directory output by default
	useDirectoryOutput := *exportAll || len(selectedTables) > 1 || (*format != "" && *output == "")
	outputFormat := models.OutputFormat(*format)
	if *format == "" {
		outputFormat = models.FormatText // default to text if no format specified
	}

	var w writer.Writer
	if useDirectoryOutput {
		w, err = writer.CreateMultiWriter(outputFormat, filename)
	} else {
		if *output == "" {
			w, err = writer.CreateWriter(outputFormat, os.Stdout)
		} else {
			out, err := os.Create(*output)
			if err != nil {
				fmt.Printf("Error creating output file: %v\n", err)
				os.Exit(1)
			}
			defer out.Close()
			w, err = writer.CreateWriter(outputFormat, out)
			if err != nil {
				fmt.Printf("Error creating writer: %v\n", err)
				os.Exit(1)
			}
		}
	}

	if err != nil {
		fmt.Printf("Error creating writer: %v\n", err)
		os.Exit(1)
	}
	defer w.Close()

	// Process each selected table
	fmt.Printf("Processing with %d workers...\n", *workers)
	for _, table := range selectedTables {
		if err := parser.ProcessSQLFileInBatches(filename, w, *workers, table); err != nil {
			fmt.Printf("Error processing table %s: %v\n", table.Name, err)
			os.Exit(1)
		}
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
