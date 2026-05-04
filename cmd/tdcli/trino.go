package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// handleTrinoQuery executes a Trino query and displays results
func handleTrinoQuery(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	if len(args) == 0 {
		return errors.New("query is required")
	}

	query := args[0]
	if flags.Verbose {
		fmt.Printf("Executing query: %s\n", query)
	}

	trinoConfig := td.TDTrinoClientConfig{
		APIKey:        flags.APIKey,
		Region:        flags.Region,
		Database:      flags.Database,
		Source:        "tdcli",
		HTTPClient:    client.HTTPClient(),
		EnableTracing: client.IsOTELEnabled(),
		Tracer:        client.GetTracer(),
		Meter:         client.GetMeter(),
	}

	trinoClient, err := td.NewTDTrinoClient(trinoConfig)
	if err != nil {
		return trinoClientError("create Trino client", err, flags)
	}
	defer trinoClient.Close()

	start := time.Now()
	rows, err := trinoClient.Query(ctx, query)
	if err != nil {
		return enhanceTrinoQueryError("execute query", err, query, flags)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return wrapErr(err, "failed to get columns", flags.Verbose)
	}

	var output io.Writer = os.Stdout
	if flags.Output != "" {
		file, err := os.Create(flags.Output)
		if err != nil {
			return wrapErr(err, "failed to create output file", flags.Verbose)
		}
		defer file.Close()
		output = file
	}

	switch strings.ToLower(flags.Format) {
	case "json":
		if err := handleTrinoQueryJSON(rows, columns, output, flags); err != nil {
			return err
		}
	case "csv":
		if err := handleTrinoQueryCSV(rows, columns, output, flags); err != nil {
			return err
		}
	case "table":
		fallthrough
	default:
		if err := handleTrinoQueryTable(rows, columns, output, flags); err != nil {
			return err
		}
	}

	if flags.Verbose {
		fmt.Printf("Query completed in %v\n", time.Since(start))
	}
	return nil
}

// handleTrinoQueryTable formats query results as a table
func handleTrinoQueryTable(rows *sql.Rows, columns []string, output io.Writer, flags Flags) error {
	fmt.Fprint(output, strings.Join(columns, "\t"))
	fmt.Fprintln(output)

	for i, col := range columns {
		if i > 0 {
			fmt.Fprint(output, "\t")
		}
		fmt.Fprint(output, strings.Repeat("-", len(col)))
	}
	fmt.Fprintln(output)

	rowCount := 0
	for rows.Next() {
		if flags.Limit > 0 && rowCount >= flags.Limit {
			break
		}

		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return wrapErr(err, "failed to scan row", flags.Verbose)
		}

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
		return wrapErr(err, "row iteration error", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Returned %d rows\n", rowCount)
	}
	return nil
}

// handleTrinoQueryTableWithPagination formats query results as a table with pagination support using buffered streaming
func handleTrinoQueryTableWithPagination(rows *sql.Rows, columns []string, output io.Writer, pageSize int) (int, error) {
	var bufferedOutput *bufio.Writer
	var isBuffered bool

	if output == os.Stdout || output == os.Stderr {
		bufferedOutput = bufio.NewWriterSize(output, 8192)
		isBuffered = true
		defer bufferedOutput.Flush()
	} else {
		bufferedOutput = bufio.NewWriterSize(output, 8192)
		isBuffered = true
		defer bufferedOutput.Flush()
	}

	actualOutput := io.Writer(bufferedOutput)
	if !isBuffered {
		actualOutput = output
	}

	fmt.Fprint(actualOutput, strings.Join(columns, "\t"))
	fmt.Fprintln(actualOutput)

	for i, col := range columns {
		if i > 0 {
			fmt.Fprint(actualOutput, "\t")
		}
		fmt.Fprint(actualOutput, strings.Repeat("-", len(col)))
	}
	fmt.Fprintln(actualOutput)

	if isBuffered {
		bufferedOutput.Flush()
	}

	totalRows := 0
	pageRows := 0
	scanner := bufio.NewScanner(os.Stdin)

	var rowBuilder strings.Builder
	rowBuilder.Grow(1024)

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return totalRows, fmt.Errorf("failed to scan row: %w", err)
		}

		rowBuilder.Reset()
		for i, val := range values {
			if i > 0 {
				rowBuilder.WriteString("\t")
			}
			if val == nil {
				rowBuilder.WriteString("NULL")
			} else {
				rowBuilder.WriteString(fmt.Sprintf("%v", val))
			}
		}
		rowBuilder.WriteString("\n")

		actualOutput.Write([]byte(rowBuilder.String()))

		totalRows++
		pageRows++

		if pageSize > 0 && pageRows >= pageSize {
			if isBuffered {
				bufferedOutput.Flush()
			}

			fmt.Printf("\n--- Page end (%d rows shown, %d total so far) ---\n", pageRows, totalRows)
			fmt.Print("Press Enter to continue, 'q' to quit, 'a' to show all: ")

			if scanner.Scan() {
				input := strings.TrimSpace(strings.ToLower(scanner.Text()))
				switch input {
				case "q", "quit":
					fmt.Printf("Query stopped. Showed %d of potentially more rows.\n", totalRows)
					return totalRows, nil
				case "a", "all":
					pageSize = 0
				}
			}
			pageRows = 0
		}

		if pageSize == 0 && totalRows%10 == 0 && isBuffered {
			bufferedOutput.Flush()
		}
	}

	if err := rows.Err(); err != nil {
		return totalRows, fmt.Errorf("row iteration error: %w", err)
	}

	return totalRows, nil
}

// handleTrinoQueryJSON formats query results as streaming JSON array
func handleTrinoQueryJSON(rows *sql.Rows, columns []string, output io.Writer, flags Flags) error {
	bufferedOutput := bufio.NewWriterSize(output, 8192)
	defer bufferedOutput.Flush()

	bufferedOutput.WriteString("[\n")

	rowCount := 0
	firstRow := true

	for rows.Next() {
		if flags.Limit > 0 && rowCount >= flags.Limit {
			break
		}

		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return wrapErr(err, "failed to scan row", flags.Verbose)
		}

		result := make(map[string]any)
		for i, col := range columns {
			if bytes, ok := values[i].([]byte); ok {
				result[col] = string(bytes)
			} else {
				result[col] = values[i]
			}
		}

		if !firstRow {
			bufferedOutput.WriteString(",\n")
		} else {
			firstRow = false
		}

		jsonBytes, err := json.MarshalIndent(result, "  ", "  ")
		if err != nil {
			return wrapErr(err, "failed to encode JSON row", flags.Verbose)
		}

		bufferedOutput.WriteString("  ")
		bufferedOutput.Write(jsonBytes)

		rowCount++

		if rowCount%100 == 0 {
			bufferedOutput.Flush()
		}
	}

	if err := rows.Err(); err != nil {
		return wrapErr(err, "row iteration error", flags.Verbose)
	}

	bufferedOutput.WriteString("\n]\n")
	bufferedOutput.Flush()

	if flags.Verbose {
		fmt.Printf("Returned %d rows\n", rowCount)
	}
	return nil
}

// handleTrinoQueryCSV formats query results as streaming CSV
func handleTrinoQueryCSV(rows *sql.Rows, columns []string, output io.Writer, flags Flags) error {
	bufferedOutput := bufio.NewWriterSize(output, 8192)
	defer bufferedOutput.Flush()

	writer := csv.NewWriter(bufferedOutput)
	defer writer.Flush()

	if err := writer.Write(columns); err != nil {
		return wrapErr(err, "failed to write CSV header", flags.Verbose)
	}
	writer.Flush()
	bufferedOutput.Flush()

	record := make([]string, len(columns))

	rowCount := 0
	for rows.Next() {
		if flags.Limit > 0 && rowCount >= flags.Limit {
			break
		}

		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return wrapErr(err, "failed to scan row", flags.Verbose)
		}

		for i, val := range values {
			if val == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := writer.Write(record); err != nil {
			return wrapErr(err, "failed to write CSV record", flags.Verbose)
		}
		rowCount++

		if rowCount%100 == 0 {
			writer.Flush()
			bufferedOutput.Flush()
		}
	}

	if err := rows.Err(); err != nil {
		return wrapErr(err, "row iteration error", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Returned %d rows\n", rowCount)
	}
	return nil
}

// handleTrinoTest tests the Trino connection
func handleTrinoTest(ctx context.Context, client *td.Client, _ []string, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	fmt.Println("Testing Trino connection...")

	trinoConfig := td.TDTrinoClientConfig{
		APIKey:        flags.APIKey,
		Region:        flags.Region,
		Database:      flags.Database,
		Source:        "tdcli",
		HTTPClient:    client.HTTPClient(),
		EnableTracing: client.IsOTELEnabled(),
		Tracer:        client.GetTracer(),
		Meter:         client.GetMeter(),
	}

	trinoClient, err := td.NewTDTrinoClient(trinoConfig)
	if err != nil {
		fmt.Printf("❌ Failed to create Trino client\n")
		fmt.Printf("Error: %v\n\n", err)
		fmt.Printf("Troubleshooting tips:\n")
		fmt.Printf("• Check if your API key is correct (format: account_id/api_key)\n")
		fmt.Printf("• Verify the region is valid: %s\n", flags.Region)
		fmt.Printf("• Ensure you have network connectivity\n")
		fmt.Printf("• Try with --verbose for more details\n")
		return fmt.Errorf("failed to create Trino client: %w", err)
	}
	defer trinoClient.Close()

	start := time.Now()
	if err := trinoClient.Ping(ctx); err != nil {
		fmt.Printf("❌ Connection test failed\n")
		fmt.Printf("Error: %v\n\n", err)
		fmt.Printf("Troubleshooting tips:\n")
		fmt.Printf("• Verify your API key has Trino query permissions\n")
		fmt.Printf("• Check if the database '%s' exists and you have access\n", flags.Database)
		fmt.Printf("• Confirm the region '%s' is correct for your account\n", flags.Region)
		fmt.Printf("• Try a different database with --database flag\n")
		fmt.Printf("• Check firewall/proxy settings if in corporate network\n")
		return fmt.Errorf("connection test failed: %w", err)
	}

	fmt.Printf("✅ Connection successful (took %v)\n", time.Since(start))
	fmt.Printf("Region: %s\n", flags.Region)
	fmt.Printf("Database: %s\n", flags.Database)
	fmt.Printf("Endpoint: %s\n", trinoClient.GetEndpoint())
	return nil
}

// handleTrinoInteractive starts an enhanced interactive Trino session with history and auto-completion
func handleTrinoInteractive(ctx context.Context, client *td.Client, _ []string, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	currentDatabase := flags.Database

	fmt.Println("Treasure Data Trino Interactive Session")
	fmt.Println("Type 'quit' or 'exit' to exit, 'help' for help")
	fmt.Printf("Database: %s, Region: %s\n", currentDatabase, flags.Region)
	fmt.Println()

	trinoConfig := td.TDTrinoClientConfig{
		APIKey:        flags.APIKey,
		Region:        flags.Region,
		Database:      currentDatabase,
		Source:        "tdcli-interactive",
		HTTPClient:    client.HTTPClient(),
		EnableTracing: client.IsOTELEnabled(),
		Tracer:        client.GetTracer(),
		Meter:         client.GetMeter(),
	}

	trinoClient, err := td.NewTDTrinoClient(trinoConfig)
	if err != nil {
		fmt.Printf("❌ Failed to create Trino client for interactive session\n")
		fmt.Printf("Error: %v\n\n", err)
		fmt.Printf("💡 Please check your connection settings and try again\n")
		fmt.Printf("  • API key: %s\n", flags.APIKey[:10]+"...")
		fmt.Printf("  • Region: %s\n", flags.Region)
		fmt.Printf("  • Database: %s\n", currentDatabase)
		return fmt.Errorf("failed to create Trino client: %w", err)
	}
	defer trinoClient.Close()

	if err := trinoClient.Ping(ctx); err != nil {
		fmt.Printf("❌ Connection test failed for interactive session\n")
		fmt.Printf("Error: %v\n\n", err)
		fmt.Printf("💡 This may indicate network issues or permission problems\n")
		fmt.Printf("  • Try: tdcli trino test --region %s --database %s\n", flags.Region, currentDatabase)
		fmt.Printf("  • Check your network connectivity\n")
		return fmt.Errorf("connection test failed: %w", err)
	}

	interactiveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	autoCompleter := newTrinoAutoCompleter(trinoClient, &currentDatabase)

	historyFile := getHistoryFile()
	rl, err := readline.NewEx(&readline.Config{
		Prompt:            fmt.Sprintf("trino:%s> ", currentDatabase),
		HistoryFile:       historyFile,
		HistoryLimit:      1000,
		AutoComplete:      autoCompleter,
		InterruptPrompt:   "^C",
		EOFPrompt:         "quit",
		HistorySearchFold: true,
	})
	if err != nil {
		return wrapErr(err, "failed to create readline", flags.Verbose)
	}
	defer rl.Close()

	for {
		rl.SetPrompt(fmt.Sprintf("trino:%s> ", currentDatabase))

		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				fmt.Println("\nGoodbye!")
				return nil
			}
			continue
		} else if err == io.EOF {
			fmt.Println("\nGoodbye!")
			return nil
		} else if err != nil {
			return wrapErr(err, "readline error", flags.Verbose)
		}

		input := strings.TrimSpace(line)
		if input == "" {
			continue
		}

		lowerInput := strings.ToLower(input)
		switch {
		case lowerInput == "quit" || lowerInput == "exit":
			fmt.Println("Goodbye!")
			return nil
		case lowerInput == "help":
			printTrinoHelp()
			continue
		case lowerInput == "clear" || lowerInput == "cls":
			readline.ClearScreen(rl)
			continue
		case lowerInput == "show databases" || lowerInput == "show schemas":
			input = "SHOW SCHEMAS"
		case lowerInput == "show tables":
			input = "SHOW TABLES"
		case strings.HasPrefix(lowerInput, "use "):
			newDB := strings.TrimSpace(input[4:])
			newDB = strings.Trim(newDB, `"'`)

			if newDB == "" {
				fmt.Println("❌ Error: Database name required. Usage: USE database_name")
				fmt.Println("   Try: USE sample_datasets")
				continue
			}

			if !switchDatabase(&trinoClient, &trinoConfig, &currentDatabase, newDB, autoCompleter, interactiveCtx) {
				continue
			}

			fmt.Printf("✅ Database changed to '%s'\n", currentDatabase)
			continue
		case lowerInput == "show current database" || lowerInput == "select database()":
			fmt.Printf("Current database: %s\n", currentDatabase)
			continue
		case strings.HasPrefix(lowerInput, "show tables from "):
			dbName := strings.TrimSpace(input[17:])
			dbName = strings.Trim(dbName, `"'`)
			input = fmt.Sprintf("SHOW TABLES FROM %s", td.EscapeIdentifier(dbName))
		case strings.HasPrefix(lowerInput, "describe "):
			tableName := strings.TrimSpace(input[9:])
			if !strings.Contains(tableName, ".") {
				tableName = fmt.Sprintf("%s.%s", td.EscapeIdentifier(currentDatabase), td.EscapeIdentifier(tableName))
			}
			input = fmt.Sprintf("DESCRIBE %s", tableName)
		}

		queryCtx, queryCancel := context.WithCancel(interactiveCtx)
		var queryDone = make(chan struct{})
		var queryErr error
		var rows *sql.Rows
		var columns []string
		var rowCount int
		start := time.Now()

		go func() {
			defer close(queryDone)
			rows, queryErr = trinoClient.Query(queryCtx, input)
			if queryErr != nil {
				return
			}

			columns, queryErr = rows.Columns()
			if queryErr != nil {
				rows.Close()
				return
			}

			rowCount, queryErr = handleTrinoQueryTableWithPagination(rows, columns, os.Stdout, 20)
			rows.Close()
		}()

		select {
		case <-queryDone:
			queryCancel()
			if queryErr != nil {
				fmt.Printf("Error: %v\n", queryErr)
			} else {
				fmt.Printf("(Query completed in %v, %d rows total)\n\n", time.Since(start), rowCount)
			}
		case sig := <-sigChan:
			fmt.Printf("\n\nReceived signal %v, cancelling query...\n", sig)
			queryCancel()
			<-queryDone
			fmt.Printf("Query cancelled after %v\n\n", time.Since(start))
		}
	}
}

// getHistoryFile returns the path to the history file
func getHistoryFile() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), ".tdcli_trino_history")
	}

	configDir := filepath.Join(homeDir, ".tdcli")
	os.MkdirAll(configDir, 0755)

	return filepath.Join(configDir, "trino_history")
}

// trinoAutoCompleter provides SQL auto-completion
type trinoAutoCompleter struct {
	client     *td.TDTrinoClient
	database   *string
	keywords   []string
	tables     map[string][]string
	tableCache time.Time
}

func newTrinoAutoCompleter(client *td.TDTrinoClient, database *string) *trinoAutoCompleter {
	keywords := []string{
		"SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "HAVING", "LIMIT",
		"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "DATABASE", "SCHEMA",
		"SHOW", "DESCRIBE", "DESC", "EXPLAIN", "USE", "WITH", "AS", "AND", "OR", "NOT",
		"IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "TRUE", "FALSE",
		"COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END",
		"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "ON", "UNION", "INTERSECT", "EXCEPT",
		"SCHEMAS", "TABLES", "COLUMNS", "CATALOGS",
	}

	return &trinoAutoCompleter{
		client:   client,
		database: database,
		keywords: keywords,
		tables:   make(map[string][]string),
	}
}

func (t *trinoAutoCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	lineStr := string(line)
	currentWord := t.getCurrentWord(lineStr, pos)

	if currentWord == "" {
		return nil, 0
	}

	suggestions := t.getSuggestions(currentWord, lineStr)
	if len(suggestions) == 0 {
		return nil, 0
	}

	result := make([][]rune, len(suggestions))
	for i, suggestion := range suggestions {
		result[i] = []rune(suggestion)
	}

	return result, len(currentWord)
}

func (t *trinoAutoCompleter) getCurrentWord(line string, pos int) string {
	if pos <= 0 || pos > len(line) {
		return ""
	}

	if pos <= len(line) && (pos == len(line) || !isWordChar(rune(line[pos-1]))) {
		return ""
	}

	start := pos - 1
	for start > 0 && isWordChar(rune(line[start-1])) {
		start--
	}

	end := pos
	for end < len(line) && isWordChar(rune(line[end])) {
		end++
	}

	if start >= end {
		return ""
	}

	return line[start:end]
}

func isWordChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
}

func (t *trinoAutoCompleter) getSuggestions(word, line string) []string {
	if word == "" {
		return []string{}
	}

	word = strings.ToUpper(word)
	var suggestions []string

	for _, keyword := range t.keywords {
		if strings.HasPrefix(keyword, word) {
			suggestions = append(suggestions, keyword)
		}
	}

	if t.isFromContext(line) {
		tables := t.getTableSuggestions(word)
		suggestions = append(suggestions, tables...)
	}

	if t.isUseContext(line) {
		databases := t.getDatabaseSuggestions(word)
		suggestions = append(suggestions, databases...)
	}

	sort.Strings(suggestions)

	return removeDuplicates(suggestions)
}

func (t *trinoAutoCompleter) isFromContext(line string) bool {
	line = strings.ToUpper(line)
	fromRegex := regexp.MustCompile(`\bFROM\s+\w*$`)
	return fromRegex.MatchString(line)
}

func (t *trinoAutoCompleter) isUseContext(line string) bool {
	line = strings.ToUpper(line)
	useRegex := regexp.MustCompile(`^\s*USE\s+\w*$`)
	return useRegex.MatchString(line)
}

func (t *trinoAutoCompleter) getTableSuggestions(word string) []string {
	if time.Since(t.tableCache) > 30*time.Second {
		t.refreshTableCache()
	}

	var suggestions []string
	word = strings.ToUpper(word)

	if t.database != nil {
		if tables, exists := t.tables[*t.database]; exists {
			for _, table := range tables {
				if strings.HasPrefix(strings.ToUpper(table), word) {
					suggestions = append(suggestions, table)
				}
			}
		}
	}

	return suggestions
}

func (t *trinoAutoCompleter) getDatabaseSuggestions(word string) []string {
	var suggestions []string
	word = strings.ToUpper(word)

	databases := t.getDatabases()
	for _, db := range databases {
		if strings.HasPrefix(strings.ToUpper(db), word) {
			suggestions = append(suggestions, db)
		}
	}

	return suggestions
}

func (t *trinoAutoCompleter) refreshTableCache() {
	if t.database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := fmt.Sprintf("SHOW TABLES FROM %s", td.EscapeIdentifier(*t.database))
	rows, err := t.client.Query(ctx, query)
	if err != nil {
		return
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tables = append(tables, tableName)
	}

	t.tables[*t.database] = tables
	t.tableCache = time.Now()
}

func (t *trinoAutoCompleter) getDatabases() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := t.client.Query(ctx, "SHOW SCHEMAS")
	if err != nil {
		return nil
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			continue
		}
		databases = append(databases, dbName)
	}

	return databases
}

func (t *trinoAutoCompleter) updateDatabase(database *string) {
	t.database = database
	t.tableCache = time.Time{}
}

func removeDuplicates(slice []string) []string {
	seen := make(map[string]bool)
	result := []string{}

	for _, str := range slice {
		if !seen[str] {
			seen[str] = true
			result = append(result, str)
		}
	}

	return result
}

// trinoClientError formats a detailed Trino client creation failure as an error.
func trinoClientError(operation string, err error, flags Flags) error {
	fmt.Printf("❌ Failed to %s\n", operation)
	fmt.Printf("Error: %v\n\n", err)

	errStr := strings.ToLower(err.Error())

	fmt.Printf("💡 Troubleshooting tips:\n")

	switch {
	case strings.Contains(errStr, "invalid api key") || strings.Contains(errStr, "unauthorized") || strings.Contains(errStr, "401"):
		fmt.Printf("  • Check if your API key is correct (format: account_id/api_key)\n")
		fmt.Printf("  • Verify the API key is active and not expired\n")
		fmt.Printf("  • Ensure the API key has Trino query permissions\n")
	case strings.Contains(errStr, "connection") || strings.Contains(errStr, "network") || strings.Contains(errStr, "timeout"):
		fmt.Printf("  • Check your network connectivity\n")
		fmt.Printf("  • Verify firewall/proxy settings if in corporate network\n")
		fmt.Printf("  • Try a different region if this one is experiencing issues\n")
	case strings.Contains(errStr, "database") || strings.Contains(errStr, "schema"):
		fmt.Printf("  • Verify the database '%s' exists\n", flags.Database)
		fmt.Printf("  • Check if you have permissions to access this database\n")
		fmt.Printf("  • Try: --database information_schema (usually accessible to all users)\n")
	case strings.Contains(errStr, "region") || strings.Contains(errStr, "endpoint"):
		fmt.Printf("  • Verify the region '%s' is correct for your account\n", flags.Region)
		fmt.Printf("  • Available regions: us, tokyo, eu, ap02, ap03\n")
		fmt.Printf("  • Check if your account has access to this region\n")
	default:
		fmt.Printf("  • Verify your API key format: account_id/api_key\n")
		fmt.Printf("  • Check if your account has Trino access enabled\n")
		fmt.Printf("  • Try with --verbose for more details\n")
		fmt.Printf("  • Contact support if the issue persists\n")
	}

	fmt.Printf("\nCurrent settings:\n")
	fmt.Printf("  Region: %s\n", flags.Region)
	fmt.Printf("  Database: %s\n", flags.Database)

	return fmt.Errorf("failed to %s: %w", operation, err)
}

// enhanceTrinoQueryError formats a detailed Trino query failure as an error.
func enhanceTrinoQueryError(operation string, err error, query string, flags Flags) error {
	fmt.Printf("❌ Failed to %s\n", operation)
	fmt.Printf("Error: %v\n", err)

	truncatedQuery := query
	if len(query) > 100 {
		truncatedQuery = query[:100] + "..."
	}
	fmt.Printf("Query: %s\n\n", truncatedQuery)

	errStr := strings.ToLower(err.Error())

	fmt.Printf("💡 Troubleshooting tips:\n")

	switch {
	case strings.Contains(errStr, "table") && (strings.Contains(errStr, "not found") || strings.Contains(errStr, "does not exist")):
		fmt.Printf("  • Check if the table name is spelled correctly\n")
		fmt.Printf("  • Verify the table exists in database '%s'\n", flags.Database)
		fmt.Printf("  • Try: SHOW TABLES to see available tables\n")
		fmt.Printf("  • Use fully qualified name: database.table if referencing other databases\n")
	case strings.Contains(errStr, "column") && (strings.Contains(errStr, "not found") || strings.Contains(errStr, "does not exist")):
		fmt.Printf("  • Check if the column name is spelled correctly\n")
		fmt.Printf("  • Column names are case-sensitive in Trino\n")
		fmt.Printf("  • Try: DESCRIBE table_name to see available columns\n")
	case strings.Contains(errStr, "permission") || strings.Contains(errStr, "access denied") || strings.Contains(errStr, "forbidden"):
		fmt.Printf("  • Check if you have SELECT permissions on the table\n")
		fmt.Printf("  • Verify your API key has the necessary access rights\n")
		fmt.Printf("  • Contact your administrator for table access\n")
	case strings.Contains(errStr, "syntax") || strings.Contains(errStr, "parsing"):
		fmt.Printf("  • Check your SQL syntax carefully\n")
		fmt.Printf("  • Trino uses ANSI SQL standard with some extensions\n")
		fmt.Printf("  • Ensure proper quoting of identifiers if needed\n")
		fmt.Printf("  • Try: EXPLAIN your_query to validate syntax\n")
	case strings.Contains(errStr, "timeout") || strings.Contains(errStr, "cancelled"):
		fmt.Printf("  • Query may be too complex or dataset too large\n")
		fmt.Printf("  • Try adding LIMIT clause to reduce result size\n")
		fmt.Printf("  • Consider breaking complex queries into smaller parts\n")
		fmt.Printf("  • Use Ctrl+C to cancel long-running queries in interactive mode\n")
	case strings.Contains(errStr, "memory") || strings.Contains(errStr, "resource"):
		fmt.Printf("  • Query requires too much memory or compute resources\n")
		fmt.Printf("  • Try adding LIMIT clause or filtering with WHERE\n")
		fmt.Printf("  • Consider using approximate functions like approx_distinct()\n")
	default:
		fmt.Printf("  • Verify the SQL syntax is correct\n")
		fmt.Printf("  • Check if all referenced tables and columns exist\n")
		fmt.Printf("  • Try a simpler query first to test connectivity\n")
		fmt.Printf("  • Use --verbose flag for more detailed error information\n")
	}

	return fmt.Errorf("failed to %s: %w", operation, err)
}

// switchDatabase handles robust database switching with comprehensive error recovery.
// Returns true on success; on failure prints diagnostics and restores the previous DB.
func switchDatabase(trinoClient **td.TDTrinoClient, config *td.TDTrinoClientConfig,
	currentDB *string, newDB string, completer *trinoAutoCompleter, ctx context.Context) bool {

	originalDB := *currentDB

	fmt.Printf("🔄 Switching to database '%s'...\\n", newDB)

	testQuery := fmt.Sprintf("SHOW TABLES FROM %s LIMIT 1", td.EscapeIdentifier(newDB))
	testRows, testErr := (*trinoClient).Query(ctx, testQuery)
	if testErr != nil {
		fmt.Printf("❌ Cannot access database '%s'\\n", newDB)
		fmt.Printf("Error: %v\\n\\n", testErr)
		fmt.Printf("💡 Troubleshooting tips:\\n")
		fmt.Printf("  • Check if database name is correct (case-sensitive)\\n")
		fmt.Printf("  • Verify you have permissions to access this database\\n")
		fmt.Printf("  • Try: SHOW SCHEMAS to see available databases\\n")
		fmt.Printf("  • Current database remains: '%s'\\n", *currentDB)
		return false
	}
	testRows.Close()

	(*trinoClient).Close()

	config.Database = newDB
	newClient, err := td.NewTDTrinoClient(*config)
	if err != nil {
		fmt.Printf("❌ Failed to create connection to database '%s'\\n", newDB)
		fmt.Printf("Error: %v\\n\\n", err)

		fmt.Printf("🔄 Recovering connection to original database '%s'...\\n", originalDB)
		config.Database = originalDB
		recoveredClient, recoverErr := td.NewTDTrinoClient(*config)
		if recoverErr != nil {
			fmt.Printf("❌ Critical: Failed to recover connection to original database '%s'\\n", originalDB)
			fmt.Printf("Recovery Error: %v\\n", recoverErr)
			fmt.Printf("\\n💡 Recovery options:\\n")
			fmt.Printf("  • Restart the interactive session\\n")
			fmt.Printf("  • Check your network connection\\n")
			fmt.Printf("  • Verify API key is still valid\\n")
			// Connection lost beyond repair; surface to caller as a fatal error.
			fmt.Println("Interactive session corrupted, exiting")
			os.Exit(1)
		}

		*trinoClient = recoveredClient
		fmt.Printf("✅ Successfully recovered connection to '%s'\\n", originalDB)

		return false
	}

	if pingErr := newClient.Ping(ctx); pingErr != nil {
		fmt.Printf("❌ Connection to database '%s' created but ping failed\\n", newDB)
		fmt.Printf("Ping Error: %v\\n\\n", pingErr)

		newClient.Close()
		fmt.Printf("🔄 Recovering connection to original database '%s'...\\n", originalDB)
		config.Database = originalDB
		recoveredClient, recoverErr := td.NewTDTrinoClient(*config)
		if recoverErr != nil {
			fmt.Printf("❌ Critical: Failed to recover connection to original database '%s'\\n", originalDB)
			fmt.Printf("Recovery Error: %v\\n", recoverErr)
			fmt.Println("Interactive session corrupted, exiting")
			os.Exit(1)
		}

		*trinoClient = recoveredClient
		fmt.Printf("✅ Successfully recovered connection to '%s'\\n", originalDB)

		return false
	}

	*trinoClient = newClient
	*currentDB = newDB
	completer.updateDatabase(currentDB)

	return true
}

// printTrinoHelp prints help for interactive mode
func printTrinoHelp() {
	fmt.Println(`
Interactive Trino Commands:
  quit, exit               - Exit the interactive session
  help                     - Show this help message
  clear, cls               - Clear the screen

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

Enhanced Features:
  Command History          - Use Up/Down arrows to navigate command history
  Auto-completion          - Press Tab for SQL keyword and table name completion
  Query Cancellation       - Press Ctrl+C to cancel running queries
  Error Recovery           - Automatic recovery from database switching failures
  Smart Error Messages     - Context-aware troubleshooting tips for common issues

Keyboard Shortcuts:
  Tab                      - Auto-complete current word
  Up/Down Arrow            - Navigate command history
  Ctrl+A                   - Move to beginning of line
  Ctrl+E                   - Move to end of line
  Ctrl+K                   - Delete from cursor to end of line
  Ctrl+U                   - Delete from cursor to beginning of line
  Ctrl+C                   - Cancel current query (during execution)
  Ctrl+C (empty line)      - Exit interactive session

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
func handleTrinoDescribe(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("table name is required")
	}

	tableName := args[0]

	escapedTable := td.EscapeIdentifier(tableName)
	query := fmt.Sprintf("DESCRIBE %s", escapedTable)

	return handleTrinoQuery(ctx, client, []string{query}, flags)
}

// handleTrinoShow executes SHOW commands
func handleTrinoShow(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("SHOW command type required (schemas, tables, columns)")
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
			return errors.New("table name required for SHOW COLUMNS")
		}
		tableName := td.EscapeIdentifier(args[1])
		query = fmt.Sprintf("SHOW COLUMNS FROM %s", tableName)
	default:
		return fmt.Errorf("unknown SHOW command: %s", showType)
	}

	return handleTrinoQuery(ctx, client, []string{query}, flags)
}

// handleTrinoExplain explains a query execution plan
func handleTrinoExplain(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("query is required for EXPLAIN")
	}

	query := fmt.Sprintf("EXPLAIN %s", args[0])

	return handleTrinoQuery(ctx, client, []string{query}, flags)
}

// handleTrinoQueryWithPagination executes a Trino query with pagination support
func handleTrinoQueryWithPagination(ctx context.Context, client *td.Client, args []string, flags Flags, pageSize int) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	if len(args) == 0 {
		return errors.New("query is required")
	}

	query := args[0]
	if flags.Verbose {
		fmt.Printf("Executing query with pagination (page size: %d): %s\n", pageSize, query)
	}

	trinoConfig := td.TDTrinoClientConfig{
		APIKey:        flags.APIKey,
		Region:        flags.Region,
		Database:      flags.Database,
		Source:        "tdcli",
		HTTPClient:    client.HTTPClient(),
		EnableTracing: client.IsOTELEnabled(),
		Tracer:        client.GetTracer(),
		Meter:         client.GetMeter(),
	}

	trinoClient, err := td.NewTDTrinoClient(trinoConfig)
	if err != nil {
		return trinoClientError("create Trino client for pagination", err, flags)
	}
	defer trinoClient.Close()

	start := time.Now()
	rows, err := trinoClient.Query(ctx, query)
	if err != nil {
		return enhanceTrinoQueryError("execute query", err, query, flags)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return wrapErr(err, "failed to get columns", flags.Verbose)
	}

	var output io.Writer = os.Stdout
	if flags.Output != "" {
		file, err := os.Create(flags.Output)
		if err != nil {
			return wrapErr(err, "failed to create output file", flags.Verbose)
		}
		defer file.Close()
		output = file
	}

	totalRows, err := handleTrinoQueryTableWithPagination(rows, columns, output, pageSize)
	if err != nil {
		return err
	}

	if flags.Verbose {
		fmt.Printf("Query completed in %v, %d rows total\n", time.Since(start), totalRows)
	}
	return nil
}

// handleTrinoVersion shows Trino version information
func handleTrinoVersion(ctx context.Context, client *td.Client, _ []string, flags Flags) error {
	query := "SELECT version()"

	return handleTrinoQuery(ctx, client, []string{query}, flags)
}
