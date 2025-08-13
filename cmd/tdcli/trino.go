package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// handleTrinoQuery executes a Trino query and displays results
func handleTrinoQuery(ctx context.Context, _ *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		log.Fatal("Query is required")
	}

	query := args[0]
	if flags.Verbose {
		fmt.Printf("Executing query: %s\n", query)
	}

	// Create Trino client
	trinoConfig := td.TDTrinoClientConfig{
		APIKey:   flags.APIKey,
		Region:   flags.Region,
		Database: flags.Database,
		Source:   "tdcli",
	}

	trinoClient, err := td.NewTDTrinoClient(trinoConfig)
	if err != nil {
		log.Fatalf("Failed to create Trino client: %v", err)
	}
	defer trinoClient.Close()

	// Execute query
	start := time.Now()
	rows, err := trinoClient.Query(ctx, query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	// Prepare output
	var output io.Writer = os.Stdout
	if flags.Output != "" {
		file, err := os.Create(flags.Output)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer file.Close()
		output = file
	}

	// Format and display results
	switch strings.ToLower(flags.Format) {
	case "json":
		handleTrinoQueryJSON(rows, columns, output, flags)
	case "csv":
		handleTrinoQueryCSV(rows, columns, output, flags)
	case "table":
		fallthrough
	default:
		handleTrinoQueryTable(rows, columns, output, flags)
	}

	if flags.Verbose {
		fmt.Printf("Query completed in %v\n", time.Since(start))
	}
}

// handleTrinoQueryTable formats query results as a table
func handleTrinoQueryTable(rows *sql.Rows, columns []string, output io.Writer, flags Flags) {
	// Print header
	fmt.Fprint(output, strings.Join(columns, "\t"))
	fmt.Fprintln(output)

	// Print separator
	for i, col := range columns {
		if i > 0 {
			fmt.Fprint(output, "\t")
		}
		fmt.Fprint(output, strings.Repeat("-", len(col)))
	}
	fmt.Fprintln(output)

	// Print rows
	rowCount := 0
	for rows.Next() {
		if flags.Limit > 0 && rowCount >= flags.Limit {
			break
		}

		// Create slice to hold column values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan row
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Print values
		for i, val := range values {
			if i > 0 {
				fmt.Fprint(output, "\t")
			}
			if val == nil {
				fmt.Fprint(output, "NULL")
			} else {
				fmt.Fprint(output, val)
			}
		}
		fmt.Fprintln(output)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
	}

	if flags.Verbose {
		fmt.Printf("Returned %d rows\n", rowCount)
	}
}

// handleTrinoQueryTableWithPagination formats query results as a table with pagination support
func handleTrinoQueryTableWithPagination(rows *sql.Rows, columns []string, output io.Writer, pageSize int) int {
	// Print header
	fmt.Fprint(output, strings.Join(columns, "\t"))
	fmt.Fprintln(output)

	// Print separator
	for i, col := range columns {
		if i > 0 {
			fmt.Fprint(output, "\t")
		}
		fmt.Fprint(output, strings.Repeat("-", len(col)))
	}
	fmt.Fprintln(output)

	totalRows := 0
	pageRows := 0
	scanner := bufio.NewScanner(os.Stdin)

	for rows.Next() {
		// Create slice to hold column values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan row
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Print values
		for i, val := range values {
			if i > 0 {
				fmt.Fprint(output, "\t")
			}
			if val == nil {
				fmt.Fprint(output, "NULL")
			} else {
				fmt.Fprint(output, val)
			}
		}
		fmt.Fprintln(output)
		totalRows++
		pageRows++

		// Check if we need to paginate (only if pageSize > 0)
		if pageSize > 0 && pageRows >= pageSize {
			fmt.Printf("\n--- Page end (%d rows shown, %d total so far) ---\n", pageRows, totalRows)
			fmt.Print("Press Enter to continue, 'q' to quit, 'a' to show all: ")

			if scanner.Scan() {
				input := strings.TrimSpace(strings.ToLower(scanner.Text()))
				if input == "q" || input == "quit" {
					fmt.Printf("Query stopped. Showed %d of potentially more rows.\n", totalRows)
					return totalRows
				} else if input == "a" || input == "all" {
					// Continue without pagination
					pageSize = 0 // Disable pagination
				}
			}
			pageRows = 0 // Reset page counter
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
	}

	return totalRows
}

// handleTrinoQueryJSON formats query results as JSON
func handleTrinoQueryJSON(rows *sql.Rows, columns []string, output io.Writer, flags Flags) {
	var results []map[string]any
	rowCount := 0

	for rows.Next() {
		if flags.Limit > 0 && rowCount >= flags.Limit {
			break
		}

		// Create slice to hold column values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan row
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Create result map
		result := make(map[string]any)
		for i, col := range columns {
			// Convert []byte to string for JSON marshaling
			if bytes, ok := values[i].([]byte); ok {
				result[col] = string(bytes)
			} else {
				result[col] = values[i]
			}
		}

		results = append(results, result)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
	}

	// Output JSON
	jsonBytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	fmt.Fprintln(output, string(jsonBytes))

	if flags.Verbose {
		fmt.Printf("Returned %d rows\n", rowCount)
	}
}

// handleTrinoQueryCSV formats query results as CSV
func handleTrinoQueryCSV(rows *sql.Rows, columns []string, output io.Writer, flags Flags) {
	writer := csv.NewWriter(output)
	defer writer.Flush()

	// Write header
	if err := writer.Write(columns); err != nil {
		log.Fatalf("Failed to write CSV header: %v", err)
	}

	// Write rows
	rowCount := 0
	for rows.Next() {
		if flags.Limit > 0 && rowCount >= flags.Limit {
			break
		}

		// Create slice to hold column values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan row
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Convert to string slice
		record := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := writer.Write(record); err != nil {
			log.Fatalf("Failed to write CSV record: %v", err)
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
	}

	if flags.Verbose {
		fmt.Printf("Returned %d rows\n", rowCount)
	}
}

// handleTrinoTest tests the Trino connection
func handleTrinoTest(ctx context.Context, _ *td.Client, _ []string, flags Flags) {
	fmt.Println("Testing Trino connection...")

	// Create Trino client
	trinoConfig := td.TDTrinoClientConfig{
		APIKey:   flags.APIKey,
		Region:   flags.Region,
		Database: flags.Database,
		Source:   "tdcli",
	}

	trinoClient, err := td.NewTDTrinoClient(trinoConfig)
	if err != nil {
		log.Fatalf("Failed to create Trino client: %v", err)
	}
	defer trinoClient.Close()

	// Test connection with a simple query
	start := time.Now()
	if err := trinoClient.Ping(ctx); err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}

	fmt.Printf("✅ Connection successful (took %v)\n", time.Since(start))
	fmt.Printf("Region: %s\n", flags.Region)
	fmt.Printf("Database: %s\n", flags.Database)
	fmt.Printf("Endpoint: %s\n", trinoClient.GetEndpoint())
}

// handleTrinoInteractive starts an interactive Trino session
func handleTrinoInteractive(ctx context.Context, _ *td.Client, _ []string, flags Flags) {
	currentDatabase := flags.Database

	fmt.Println("Treasure Data Trino Interactive Session")
	fmt.Println("Type 'quit' or 'exit' to exit, 'help' for help")
	fmt.Printf("Database: %s, Region: %s\n", currentDatabase, flags.Region)
	fmt.Println()

	// Create initial Trino client
	trinoConfig := td.TDTrinoClientConfig{
		APIKey:   flags.APIKey,
		Region:   flags.Region,
		Database: currentDatabase,
		Source:   "tdcli-interactive",
	}

	trinoClient, err := td.NewTDTrinoClient(trinoConfig)
	if err != nil {
		log.Fatalf("Failed to create Trino client: %v", err)
	}
	defer trinoClient.Close()

	// Test connection
	if err := trinoClient.Ping(ctx); err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}

	scanner := bufio.NewScanner(os.Stdin)

	for {
		// Show current database in prompt
		fmt.Printf("trino:%s> ", currentDatabase)
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		// Handle special commands
		lowerInput := strings.ToLower(input)
		switch {
		case lowerInput == "quit" || lowerInput == "exit":
			fmt.Println("Goodbye!")
			return
		case lowerInput == "help":
			printTrinoHelp()
			continue
		case lowerInput == "show databases" || lowerInput == "show schemas":
			input = "SHOW SCHEMAS"
		case lowerInput == "show tables":
			input = "SHOW TABLES"
		case strings.HasPrefix(lowerInput, "use "):
			// Handle database switching
			newDB := strings.TrimSpace(input[4:]) // Remove "use "
			newDB = strings.Trim(newDB, `"'`)     // Remove quotes if present

			if newDB == "" {
				fmt.Println("Error: Database name required. Usage: USE database_name")
				continue
			}

			// Test if database exists by trying to show tables
			testQuery := fmt.Sprintf("SHOW TABLES FROM %s LIMIT 1", td.EscapeIdentifier(newDB))
			testRows, testErr := trinoClient.Query(ctx, testQuery)
			if testErr != nil {
				fmt.Printf("Error: Cannot switch to database '%s': %v\n", newDB, testErr)
				continue
			}
			testRows.Close()

			// Create new client with different database
			trinoClient.Close()
			trinoConfig.Database = newDB
			trinoClient, err = td.NewTDTrinoClient(trinoConfig)
			if err != nil {
				fmt.Printf("Error: Failed to switch to database '%s': %v\n", newDB, err)
				// Try to recreate with original database
				trinoConfig.Database = currentDatabase
				trinoClient, _ = td.NewTDTrinoClient(trinoConfig)
				continue
			}

			currentDatabase = newDB
			fmt.Printf("Database changed to '%s'\n", currentDatabase)
			continue
		case lowerInput == "show current database" || lowerInput == "select database()":
			fmt.Printf("Current database: %s\n", currentDatabase)
			continue
		case strings.HasPrefix(lowerInput, "show tables from "):
			// Extract database name and show tables
			dbName := strings.TrimSpace(input[17:]) // Remove "show tables from "
			dbName = strings.Trim(dbName, `"'`)     // Remove quotes
			input = fmt.Sprintf("SHOW TABLES FROM %s", td.EscapeIdentifier(dbName))
		case strings.HasPrefix(lowerInput, "describe "):
			// Enhance describe to work with current database context
			tableName := strings.TrimSpace(input[9:]) // Remove "describe "
			if !strings.Contains(tableName, ".") {
				// If no database specified, use current database
				tableName = fmt.Sprintf("%s.%s", td.EscapeIdentifier(currentDatabase), td.EscapeIdentifier(tableName))
			}
			input = fmt.Sprintf("DESCRIBE %s", tableName)
		}

		// Execute query
		start := time.Now()
		rows, err := trinoClient.Query(ctx, input)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		// Get columns
		columns, err := rows.Columns()
		if err != nil {
			fmt.Printf("Error getting columns: %v\n", err)
			rows.Close()
			continue
		}

		// Display results in table format with pagination
		rowCount := handleTrinoQueryTableWithPagination(rows, columns, os.Stdout, 20) // 20 rows per page
		rows.Close()

		fmt.Printf("(Query completed in %v, %d rows total)\n\n", time.Since(start), rowCount)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
}

// printTrinoHelp prints help for interactive mode
func printTrinoHelp() {
	fmt.Println(`
Interactive Trino Commands:
  quit, exit               - Exit the interactive session
  help                     - Show this help message
  
Database Commands:
  show databases           - List all available databases
  show schemas             - Same as show databases
  use <database>           - Switch to a different database
  show current database    - Show the current database name
  show tables              - List tables in current database
  show tables from <db>    - List tables in specified database
  
SQL Commands:
  SELECT ...               - Execute SELECT queries
  DESCRIBE <table>         - Show table structure (uses current database)
  DESCRIBE <db>.<table>    - Show table structure from specific database
  SHOW SCHEMAS             - List all schemas/databases
  SHOW TABLES              - List tables in current schema
  SHOW TABLES FROM <db>    - List tables from specific database
  
Pagination Controls (for large result sets):
  Enter             - Show next page
  q, quit           - Stop query and exit pagination
  a, all            - Show all remaining rows without pagination

Examples:
  use sample_datasets;
  show tables;
  SELECT COUNT(*) FROM nasdaq;
  use information_schema;
  show tables;
  DESCRIBE sample_datasets.nasdaq;
  SELECT * FROM nasdaq LIMIT 10;`)
}

// handleTrinoDescribe describes a table structure
func handleTrinoDescribe(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		log.Fatal("Table name is required")
	}

	tableName := args[0]

	// Escape table name for safety
	escapedTable := td.EscapeIdentifier(tableName)
	query := fmt.Sprintf("DESCRIBE %s", escapedTable)

	// Execute as a regular query
	handleTrinoQuery(ctx, client, []string{query}, flags)
}

// handleTrinoShow executes SHOW commands
func handleTrinoShow(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		log.Fatal("SHOW command type required (schemas, tables, columns)")
	}

	showType := strings.ToLower(args[0])
	var query string

	switch showType {
	case "schemas", "databases":
		query = "SHOW SCHEMAS"
	case "tables":
		if flags.Database != "" {
			query = fmt.Sprintf("SHOW TABLES FROM %s", td.EscapeIdentifier(flags.Database))
		} else {
			query = "SHOW TABLES"
		}
	case "columns":
		if len(args) < 2 {
			log.Fatal("Table name required for SHOW COLUMNS")
		}
		tableName := td.EscapeIdentifier(args[1])
		query = fmt.Sprintf("SHOW COLUMNS FROM %s", tableName)
	default:
		log.Fatalf("Unknown SHOW command: %s", showType)
	}

	// Execute as a regular query
	handleTrinoQuery(ctx, client, []string{query}, flags)
}

// handleTrinoExplain explains a query execution plan
func handleTrinoExplain(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		log.Fatal("Query is required for EXPLAIN")
	}

	query := fmt.Sprintf("EXPLAIN %s", args[0])

	// Execute as a regular query
	handleTrinoQuery(ctx, client, []string{query}, flags)
}

// handleTrinoQueryWithPagination executes a Trino query with pagination support
func handleTrinoQueryWithPagination(ctx context.Context, _ *td.Client, args []string, flags Flags, pageSize int) {
	if len(args) == 0 {
		log.Fatal("Query is required")
	}

	query := args[0]
	if flags.Verbose {
		fmt.Printf("Executing query with pagination (page size: %d): %s\n", pageSize, query)
	}

	// Create Trino client
	trinoConfig := td.TDTrinoClientConfig{
		APIKey:   flags.APIKey,
		Region:   flags.Region,
		Database: flags.Database,
		Source:   "tdcli",
	}

	trinoClient, err := td.NewTDTrinoClient(trinoConfig)
	if err != nil {
		log.Fatalf("Failed to create Trino client: %v", err)
	}
	defer trinoClient.Close()

	// Execute query
	start := time.Now()
	rows, err := trinoClient.Query(ctx, query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	// Prepare output
	var output io.Writer = os.Stdout
	if flags.Output != "" {
		file, err := os.Create(flags.Output)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer file.Close()
		output = file
	}

	// Format and display results with pagination
	totalRows := handleTrinoQueryTableWithPagination(rows, columns, output, pageSize)

	if flags.Verbose {
		fmt.Printf("Query completed in %v, %d rows total\n", time.Since(start), totalRows)
	}
}

// handleTrinoVersion shows Trino version information
func handleTrinoVersion(ctx context.Context, client *td.Client, _ []string, flags Flags) {
	query := "SELECT version()"

	// Execute as a regular query
	handleTrinoQuery(ctx, client, []string{query}, flags)
}
