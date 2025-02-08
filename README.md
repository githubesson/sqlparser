# SQL Parser

A high-performance SQL INSERT statement parser that processes large SQL files and outputs the data in various formats.

## Features

- Processes large SQL files with INSERT statements
- Memory-efficient batch processing
- Parallel processing with configurable worker count
- Multiple output formats:
  - JSON
  - JSONL
  - CSV
  - Text
- Buffered I/O for optimal performance
- Configurable batch size and worker count via environment variables

## Installation

```bash
git clone https://github.com/githubesson/sqlparser
cd sqlparser
go build cmd/sqlparser/main.go
```

## Usage

```bash
sqlparser [-format=txt|csv|json|jsonl] [-output=filename] [-workers=N] <sqlfile>
```

### Arguments

- `-format`: Output format (default: txt)
  - `txt`: Human-readable text format
  - `csv`: CSV format with headers
  - `json`: JSON format with table structure
  - `jsonl`: JSON lines format with table structure
- `-output`: Output file path (default: stdout)
- `-workers`: Number of worker threads (default: 1)
- `<sqlfile>`: Input SQL file containing INSERT statements

### Environment Variables

The application can be configured using environment variables in a `.env` file:

```env
BATCH_SIZE=100000  # Number of rows to process in each batch
WORKER_COUNT=1     # Default number of worker threads
```

### Examples

1. Process SQL file and output as JSON:
```bash
sqlparser -format=json -output=output.json input.sql
```

2. Process SQL file with 4 workers and output as CSV:
```bash
sqlparser -format=csv -workers=4 -output=output.csv input.sql
```

3. Process SQL file and print to console in text format:
```bash
sqlparser input.sql
```

4. Process SQL file and output as JSON lines:
```bash
sqlparser -format=jsonl -output=output.json input.sql
```

## Performance Optimization

The parser is optimized for performance through:
- Batch processing to manage memory usage
- Parallel processing with configurable worker count
- Buffered I/O operations
- Memory pooling for row data
- Efficient string handling

## Output Formats

### Text Format
```
Table: users
Row 1:
  id: 1
  name: John Doe
  email: john@example.com
```

### CSV Format
```
Table:,users
Row,id,name,email
1,1,John Doe,john@example.com
```

### JSON Format
```json
[
  {
    "table_name": "users",
    "rows": [
      {
        "row_number": 1,
        "data": {
          "id": "1",
          "name": "John Doe",
          "email": "john@example.com"
        }
      }
    ]
  }
]
```

### JSONL Format
```json
{"table_name": "users", "rows": [{"row_number": 1, "data": {"id": "1", "name": "John Doe", "email": "john@example.com"}}]}
{"table_name": "users", "rows": [{"row_number": 2, "data": {"id": "2", "name": "John Doe", "email": "john@example.com"}}]}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 