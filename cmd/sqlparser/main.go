package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"sqlparser/pkg/models"
	"sqlparser/pkg/parser"
	"sqlparser/pkg/writer"
)

func main() {
	format := flag.String("format", "txt", "Output format (txt, csv, json)")
	output := flag.String("output", "", "Output file (if not specified, prints to stdout)")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of worker threads (default: number of CPU cores)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Printf("Usage: sqlparser [-format=txt|csv|json] [-output=filename] [-workers=N] <sqlfile>\n")
		fmt.Printf("  -format: Output format (default: txt)\n")
		fmt.Printf("  -output: Output file (default: stdout)\n")
		fmt.Printf("  -workers: Number of worker threads (default: %d)\n", runtime.NumCPU())
		os.Exit(1)
	}

	filename := args[0]
	outputFormat := models.OutputFormat(*format)

	// Create output file or use stdout
	var out *os.File
	var err error
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
	err = parser.ProcessSQLFileInBatches(filename, w, *workers)
	if err != nil {
		fmt.Printf("Error processing SQL file: %v\n", err)
		os.Exit(1)
	}
}
