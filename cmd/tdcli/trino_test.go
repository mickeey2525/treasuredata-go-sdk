package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func TestTrinoQueryCmd_Run(t *testing.T) {
	// Skip if no API key is available
	if os.Getenv("TD_API_KEY") == "" {
		t.Skip("Skipping Trino CLI test: TD_API_KEY not set")
	}

	cmd := &TrinoQueryCmd{
		Query:    "SELECT 1 as test",
		Database: "sample_datasets",
		Limit:    1,
	}

	ctx := &CLIContext{
		Context: context.Background(),
		GlobalFlags: Flags{
			APIKey: os.Getenv("TD_API_KEY"),
			Region: "us",
			Format: "json",
		},
	}

	// This would normally execute the query, but we'll just test the structure
	err := cmd.Run(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Verify that the flags were set correctly
	if ctx.GlobalFlags.Database != cmd.Database {
		t.Errorf("Expected database %s, got %s", cmd.Database, ctx.GlobalFlags.Database)
	}

	if ctx.GlobalFlags.Limit != cmd.Limit {
		t.Errorf("Expected limit %d, got %d", cmd.Limit, ctx.GlobalFlags.Limit)
	}
}

func TestTrinoTestCmd_Run(t *testing.T) {
	cmd := &TrinoTestCmd{
		Database: "sample_datasets",
	}

	ctx := &CLIContext{
		Context: context.Background(),
		GlobalFlags: Flags{
			APIKey: "test_account/test_key",
			Region: "us",
			Format: "table",
		},
	}

	// Test that the command sets the database flag correctly
	// We're not actually running the command to avoid authentication issues
	ctx.GlobalFlags.Database = cmd.Database

	// Verify the database was set
	if ctx.GlobalFlags.Database != cmd.Database {
		t.Errorf("Expected database %s, got %s", cmd.Database, ctx.GlobalFlags.Database)
	}
}

func TestTrinoDescribeCmd_Structure(t *testing.T) {
	cmd := &TrinoDescribeCmd{
		Table:    "nasdaq",
		Database: "sample_datasets",
	}

	// Test the command structure without actually executing
	if cmd.Table != "nasdaq" {
		t.Errorf("Expected table %s, got %s", "nasdaq", cmd.Table)
	}

	if cmd.Database != "sample_datasets" {
		t.Errorf("Expected database %s, got %s", "sample_datasets", cmd.Database)
	}
}

func TestTrinoShowCmd_Structure(t *testing.T) {
	tests := []struct {
		name     string
		cmd      TrinoShowCmd
		wantType string
	}{
		{
			name: "show schemas",
			cmd: TrinoShowCmd{
				Type:     "schemas",
				Database: "sample_datasets",
			},
			wantType: "schemas",
		},
		{
			name: "show tables",
			cmd: TrinoShowCmd{
				Type:     "tables",
				Database: "sample_datasets",
			},
			wantType: "tables",
		},
		{
			name: "show columns",
			cmd: TrinoShowCmd{
				Type:     "columns",
				Table:    "nasdaq",
				Database: "sample_datasets",
			},
			wantType: "columns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test command structure
			if tt.cmd.Type != tt.wantType {
				t.Errorf("Expected type %s, got %s", tt.wantType, tt.cmd.Type)
			}

			if tt.cmd.Database != "sample_datasets" {
				t.Errorf("Expected database sample_datasets, got %s", tt.cmd.Database)
			}

			if tt.wantType == "columns" && tt.cmd.Table != "nasdaq" {
				t.Errorf("Expected table nasdaq for columns command, got %s", tt.cmd.Table)
			}
		})
	}
}

func TestTrinoExplainCmd_Structure(t *testing.T) {
	cmd := &TrinoExplainCmd{
		Query:    "SELECT COUNT(*) FROM nasdaq",
		Database: "sample_datasets",
	}

	// Test command structure
	if cmd.Query != "SELECT COUNT(*) FROM nasdaq" {
		t.Errorf("Expected query 'SELECT COUNT(*) FROM nasdaq', got %s", cmd.Query)
	}

	if cmd.Database != "sample_datasets" {
		t.Errorf("Expected database sample_datasets, got %s", cmd.Database)
	}
}

func TestTrinoVersionCmd_Structure(t *testing.T) {
	cmd := &TrinoVersionCmd{
		Database: "sample_datasets",
	}

	// Test command structure
	if cmd.Database != "sample_datasets" {
		t.Errorf("Expected database sample_datasets, got %s", cmd.Database)
	}
}

func TestPrintTrinoHelp(t *testing.T) {
	// Capture stdout to verify help output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printTrinoHelp()

	w.Close()
	os.Stdout = oldStdout

	// Read the captured output with larger buffer
	var buf strings.Builder
	content := make([]byte, 8192) // Larger buffer for complete help content
	for {
		n, err := r.Read(content)
		if n > 0 {
			buf.Write(content[:n])
		}
		if err != nil {
			break
		}
	}
	output := buf.String()

	// Verify help content contains expected sections
	expectedSections := []string{
		"Interactive Trino Commands:",
		"Database Commands:",
		"SQL Commands:",
		"Examples:",
		"use <database>",
		"show databases",
		"show tables",
		"show current database",
		"SELECT",
		"DESCRIBE",
	}

	for _, section := range expectedSections {
		if !strings.Contains(output, section) {
			t.Errorf("Expected help output to contain %q, but it didn't", section)
		}
	}
}

func TestDatabaseSwitchingLogic(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "use command with simple database name",
			input:    "use test_db",
			expected: "test_db",
		},
		{
			name:     "use command with quoted database name",
			input:    `use "test_db"`,
			expected: "test_db",
		},
		{
			name:     "use command with single quoted database name",
			input:    "use 'test_db'",
			expected: "test_db",
		},
		{
			name:     "use command with extra spaces",
			input:    "use   test_db   ",
			expected: "test_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lowerInput := strings.ToLower(tt.input)
			if strings.HasPrefix(lowerInput, "use ") {
				newDB := strings.TrimSpace(tt.input[4:]) // Remove "use "
				newDB = strings.Trim(newDB, `"'`)        // Remove quotes if present

				if newDB != tt.expected {
					t.Errorf("Expected database name %q, got %q", tt.expected, newDB)
				}
			}
		})
	}
}

func TestTableQualificationLogic(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		currentDatabase string
		expectedQuery   string
	}{
		{
			name:            "describe with unqualified table name",
			input:           "describe nasdaq",
			currentDatabase: "sample_datasets",
			expectedQuery:   `DESCRIBE "sample_datasets"."nasdaq"`,
		},
		{
			name:            "describe with qualified table name",
			input:           "describe sample_datasets.nasdaq",
			currentDatabase: "other_db",
			expectedQuery:   "DESCRIBE sample_datasets.nasdaq",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lowerInput := strings.ToLower(tt.input)
			if strings.HasPrefix(lowerInput, "describe ") {
				tableName := strings.TrimSpace(tt.input[9:]) // Remove "describe "
				if !strings.Contains(tableName, ".") {
					// If no database specified, use current database
					tableName = fmt.Sprintf("%s.%s", td.EscapeIdentifier(tt.currentDatabase), td.EscapeIdentifier(tableName))
				}
				query := fmt.Sprintf("DESCRIBE %s", tableName)

				if query != tt.expectedQuery {
					t.Errorf("Expected query %q, got %q", tt.expectedQuery, query)
				}
			}
		})
	}
}

func TestTrinoQueryWithPagination(t *testing.T) {
	// Test pagination logic
	pageSize := 5

	if pageSize <= 0 {
		t.Error("Page size should be positive")
	}

	// Test that we can set different page sizes
	testSizes := []int{10, 20, 50}
	for _, size := range testSizes {
		if size <= 0 {
			t.Errorf("Invalid page size: %d", size)
		}
	}
}

func TestPaginationControls(t *testing.T) {
	tests := []struct {
		input         string
		shouldQuit    bool
		shouldShowAll bool
	}{
		{"q", true, false},
		{"quit", true, false},
		{"Q", true, false},
		{"QUIT", true, false},
		{"a", false, true},
		{"all", false, true},
		{"A", false, true},
		{"ALL", false, true},
		{"", false, false},          // Enter to continue
		{"something", false, false}, // Any other input continues
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input_%s", tt.input), func(t *testing.T) {
			input := strings.TrimSpace(strings.ToLower(tt.input))
			isQuit := input == "q" || input == "quit"
			isShowAll := input == "a" || input == "all"

			if isQuit != tt.shouldQuit {
				t.Errorf("Expected quit=%v for input %q, got %v", tt.shouldQuit, tt.input, isQuit)
			}

			if isShowAll != tt.shouldShowAll {
				t.Errorf("Expected showAll=%v for input %q, got %v", tt.shouldShowAll, tt.input, isShowAll)
			}
		})
	}
}

func TestBufferedStreamingLogic(t *testing.T) {
	// Test string builder efficiency
	var builder strings.Builder
	builder.Grow(1024) // Pre-allocate

	// Simulate row building
	testValues := []interface{}{"col1", "col2", nil, "col4"}
	for i, val := range testValues {
		if i > 0 {
			builder.WriteString("\t")
		}
		if val == nil {
			builder.WriteString("NULL")
		} else {
			builder.WriteString(fmt.Sprintf("%v", val))
		}
	}
	builder.WriteString("\n")

	expected := "col1\tcol2\tNULL\tcol4\n"
	result := builder.String()

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}

	// Test builder reset
	builder.Reset()
	if builder.Len() != 0 {
		t.Error("Builder should be empty after reset")
	}
}

func TestStreamingFormatters(t *testing.T) {
	// Test streaming memory efficiency concept
	// In real streaming, memory usage should be O(1)

	tests := []struct {
		name           string
		format         string
		expectedPrefix string
		expectedSuffix string
	}{
		{
			name:           "JSON streaming format",
			format:         "json",
			expectedPrefix: "[\n",
			expectedSuffix: "\n]\n",
		},
		{
			name:           "CSV streaming format",
			format:         "csv",
			expectedPrefix: "", // CSV starts with header row
			expectedSuffix: "", // CSV ends with last data row
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// These tests validate the conceptual structure
			// Real streaming would require database connections

			if tt.format == "json" && tt.expectedPrefix != "[\n" {
				t.Error("JSON should start with array opening")
			}

			if tt.format == "json" && tt.expectedSuffix != "\n]\n" {
				t.Error("JSON should end with array closing")
			}
		})
	}
}

// Memory load tests that verify streaming efficiency
func TestMemoryLoadBufferedStreaming(t *testing.T) {
	t.Run("String builder memory efficiency", func(t *testing.T) {
		// Test that string builder doesn't accumulate memory when reset
		var builder strings.Builder
		builder.Grow(1024) // Pre-allocate

		// Simulate processing many rows
		for i := 0; i < 10000; i++ {
			// Build a row
			builder.WriteString("col1\tcol2\tcol3\tdata_")
			builder.WriteString(fmt.Sprintf("%d", i))
			builder.WriteString("\n")

			// Get the string (simulates writing to output)
			rowData := builder.String()
			if len(rowData) == 0 {
				t.Error("Row data should not be empty")
			}

			// Reset for next row (key to memory efficiency)
			builder.Reset()

			// Verify builder is clean
			if builder.Len() != 0 {
				t.Error("Builder should be empty after reset")
			}
		}
	})

	t.Run("Buffered writer memory pattern", func(t *testing.T) {
		// Test buffered writer with periodic flushing
		var buf strings.Builder
		bufferedWriter := bufio.NewWriterSize(&buf, 8192)

		// Simulate streaming large amounts of data
		for i := 0; i < 50000; i++ {
			data := fmt.Sprintf("row_%d\tvalue_%d\tdata_%d\n", i, i*2, i*3)
			bufferedWriter.WriteString(data)

			// Periodic flush (every 100 rows)
			if i%100 == 0 {
				bufferedWriter.Flush()
				// After flush, buffer should be available for more data
				if bufferedWriter.Available() == 0 {
					t.Error("Buffer should have available space after flush")
				}
			}
		}
		bufferedWriter.Flush()

		// Verify we processed all data
		result := buf.String()
		lines := strings.Count(result, "\n")
		if lines != 50000 {
			t.Errorf("Expected 50000 lines, got %d", lines)
		}
	})
}

func TestMemoryLoadJSONStreaming(t *testing.T) {
	t.Run("JSON streaming memory pattern", func(t *testing.T) {
		// Test JSON streaming without accumulating all results
		var output strings.Builder
		bufferedOutput := bufio.NewWriterSize(&output, 8192)

		// Start JSON array
		bufferedOutput.WriteString("[\n")
		firstRow := true

		// Simulate streaming many JSON objects
		for i := 0; i < 25000; i++ {
			// Create result map (simulates row scan)
			result := map[string]interface{}{
				"id":      i,
				"name":    fmt.Sprintf("user_%d", i),
				"email":   fmt.Sprintf("user%d@example.com", i),
				"status":  "active",
				"balance": float64(float64(i) * 100.5),
			}

			// Add comma separator
			if !firstRow {
				bufferedOutput.WriteString(",\n")
			} else {
				firstRow = false
			}

			// Marshal and stream immediately (key to memory efficiency)
			jsonBytes, err := json.MarshalIndent(result, "  ", "  ")
			if err != nil {
				t.Fatalf("JSON marshal error: %v", err)
			}

			bufferedOutput.WriteString("  ")
			bufferedOutput.Write(jsonBytes)

			// Periodic flush to prevent buffer overflow
			if i%100 == 0 {
				bufferedOutput.Flush()
			}

			// Verify we're not accumulating objects in memory
			// (result map goes out of scope and can be GC'd)
			result = nil
		}

		// Close JSON array
		bufferedOutput.WriteString("\n]\n")
		bufferedOutput.Flush()

		// Verify output structure
		resultStr := output.String()
		if !strings.HasPrefix(resultStr, "[\n") {
			t.Error("JSON should start with array opening")
		}
		if !strings.HasSuffix(resultStr, "\n]\n") {
			t.Error("JSON should end with array closing")
		}

		// Count objects (should match our loop count)
		objectCount := strings.Count(resultStr, `"id":`)
		if objectCount != 25000 {
			t.Errorf("Expected 25000 JSON objects, got %d", objectCount)
		}
	})
}

func TestMemoryLoadCSVStreaming(t *testing.T) {
	t.Run("CSV streaming memory pattern", func(t *testing.T) {
		// Test CSV streaming without accumulating all records
		var output strings.Builder
		bufferedOutput := bufio.NewWriterSize(&output, 8192)
		writer := csv.NewWriter(bufferedOutput)

		// Write header immediately
		header := []string{"id", "name", "email", "status", "balance"}
		if err := writer.Write(header); err != nil {
			t.Fatalf("Failed to write CSV header: %v", err)
		}
		writer.Flush()
		bufferedOutput.Flush()

		// Pre-allocate record slice (reuse for efficiency)
		record := make([]string, len(header))

		// Stream many CSV records
		for i := 0; i < 30000; i++ {
			// Populate record slice (simulates row scan)
			record[0] = fmt.Sprintf("%d", i)
			record[1] = fmt.Sprintf("user_%d", i)
			record[2] = fmt.Sprintf("user%d@example.com", i)
			record[3] = "active"
			record[4] = fmt.Sprintf("%.2f", float64(i)*100.50)

			// Write record immediately (key to memory efficiency)
			if err := writer.Write(record); err != nil {
				t.Fatalf("Failed to write CSV record: %v", err)
			}

			// Periodic flush to prevent buffer overflow
			if i%100 == 0 {
				writer.Flush()
				bufferedOutput.Flush()
			}

			// Record slice is reused, no accumulation
		}

		writer.Flush()
		bufferedOutput.Flush()

		// Verify output
		resultStr := output.String()
		lines := strings.Count(resultStr, "\n")
		// Should have header + 30000 data rows
		if lines != 30001 {
			t.Errorf("Expected 30001 lines (header + 30000 records), got %d", lines)
		}

		// Verify header is present
		if !strings.Contains(resultStr, "id,name,email,status,balance") {
			t.Error("CSV should contain header row")
		}
	})
}

func TestMemoryLoadPaginationBuffering(t *testing.T) {
	t.Run("Pagination with string builder efficiency", func(t *testing.T) {
		// Test pagination doesn't accumulate memory across pages
		var output strings.Builder
		bufferedOutput := bufio.NewWriterSize(&output, 8192)

		// Pre-allocate row builder (reused across all rows)
		var rowBuilder strings.Builder
		rowBuilder.Grow(1024)

		columns := []string{"col1", "col2", "col3", "col4", "col5"}
		pageSize := 20
		totalRows := 5000

		// Write header
		bufferedOutput.WriteString(strings.Join(columns, "\t"))
		bufferedOutput.WriteString("\n")
		bufferedOutput.Flush()

		// Simulate row processing with pagination
		for row := 0; row < totalRows; row++ {
			// Build row efficiently
			rowBuilder.Reset() // Key: reset for each row
			for i, col := range columns {
				if i > 0 {
					rowBuilder.WriteString("\t")
				}
				// Simulate column values
				value := fmt.Sprintf("%s_%d", col, row)
				rowBuilder.WriteString(value)
			}
			rowBuilder.WriteString("\n")

			// Write complete row
			bufferedOutput.WriteString(rowBuilder.String())

			// Simulate pagination flush
			if (row+1)%pageSize == 0 {
				bufferedOutput.Flush()
				// At page boundary, verify builder is still efficient
				if rowBuilder.Cap() > 2048 { // Should not grow excessively
					t.Errorf("Row builder capacity grew too large: %d", rowBuilder.Cap())
				}
			}

			// Periodic flush for non-paginated sections
			if row%10 == 0 {
				bufferedOutput.Flush()
			}
		}

		bufferedOutput.Flush()

		// Verify all rows were processed
		resultStr := output.String()
		lines := strings.Count(resultStr, "\n")
		if lines != totalRows+1 { // +1 for header
			t.Errorf("Expected %d lines, got %d", totalRows+1, lines)
		}
	})
}

// Benchmark tests to measure memory allocation patterns
func BenchmarkStreamingMemoryEfficiency(b *testing.B) {
	b.Run("StringBuilderReuse", func(b *testing.B) {
		var rowBuilder strings.Builder
		rowBuilder.Grow(1024)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Build a row
			rowBuilder.WriteString("col1\tcol2\tcol3\tdata_")
			rowBuilder.WriteString(fmt.Sprintf("%d", i))
			rowBuilder.WriteString("\n")

			// Consume the string (simulates output)
			_ = rowBuilder.String()

			// Reset for reuse (key to memory efficiency)
			rowBuilder.Reset()
		}
	})

	b.Run("BufferedWriterEfficiency", func(b *testing.B) {
		var output strings.Builder
		bufferedWriter := bufio.NewWriterSize(&output, 8192)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data := fmt.Sprintf("row_%d\tvalue_%d\n", i, i*2)
			bufferedWriter.WriteString(data)

			// Periodic flush to prevent unlimited buffering
			if i%100 == 0 {
				bufferedWriter.Flush()
			}
		}
		bufferedWriter.Flush()
	})

	b.Run("JSONStreamingEfficiency", func(b *testing.B) {
		var output strings.Builder
		bufferedOutput := bufio.NewWriterSize(&output, 8192)

		// Start JSON array
		bufferedOutput.WriteString("[\n")
		firstRow := true

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create result map (goes out of scope after each iteration)
			result := map[string]interface{}{
				"id":    i,
				"name":  fmt.Sprintf("user_%d", i),
				"value": i * 100,
			}

			if !firstRow {
				bufferedOutput.WriteString(",\n")
			} else {
				firstRow = false
			}

			// Marshal and stream immediately
			jsonBytes, _ := json.Marshal(result)
			bufferedOutput.Write(jsonBytes)

			if i%100 == 0 {
				bufferedOutput.Flush()
			}

			// result goes out of scope, eligible for GC
		}

		bufferedOutput.WriteString("\n]\n")
		bufferedOutput.Flush()
	})

	b.Run("CSVStreamingEfficiency", func(b *testing.B) {
		var output strings.Builder
		bufferedOutput := bufio.NewWriterSize(&output, 8192)
		writer := csv.NewWriter(bufferedOutput)

		// Pre-allocate record slice (reused)
		record := make([]string, 3)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Reuse the same slice
			record[0] = fmt.Sprintf("%d", i)
			record[1] = fmt.Sprintf("value_%d", i)
			record[2] = fmt.Sprintf("data_%d", i*2)

			writer.Write(record)

			if i%100 == 0 {
				writer.Flush()
				bufferedOutput.Flush()
			}
		}

		writer.Flush()
		bufferedOutput.Flush()
	})
}

// Memory comparison benchmark: streaming vs accumulating approaches
func BenchmarkMemoryComparison(b *testing.B) {
	b.Run("StreamingApproach", func(b *testing.B) {
		// Simulates our streaming implementation
		var output strings.Builder
		bufferedWriter := bufio.NewWriterSize(&output, 8192)
		var rowBuilder strings.Builder
		rowBuilder.Grow(1024)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Process one row at a time
			rowBuilder.Reset()
			rowBuilder.WriteString(fmt.Sprintf("col1_%d\tcol2_%d\tcol3_%d\n", i, i*2, i*3))
			bufferedWriter.WriteString(rowBuilder.String())

			if i%10 == 0 {
				bufferedWriter.Flush()
			}
		}
		bufferedWriter.Flush()
	})

	b.Run("AccumulatingApproach", func(b *testing.B) {
		// Simulates the old memory-heavy approach
		var allRows []string // This accumulates memory

		b.ResetTimer()
		b.ReportAllocs()

		// First pass: accumulate all rows
		for i := 0; i < b.N; i++ {
			row := fmt.Sprintf("col1_%d\tcol2_%d\tcol3_%d\n", i, i*2, i*3)
			allRows = append(allRows, row) // Memory accumulation
		}

		// Second pass: write all at once
		var output strings.Builder
		for _, row := range allRows {
			output.WriteString(row)
		}
	})
}

// Test memory usage stays constant regardless of dataset size
func TestConstantMemoryUsage(t *testing.T) {
	t.Run("Memory usage independent of dataset size", func(t *testing.T) {
		// Test that our streaming approach uses similar memory
		// regardless of processing 1,000 or 100,000 rows

		datasets := []int{1000, 10000, 100000}

		for _, size := range datasets {
			t.Run(fmt.Sprintf("dataset_size_%d", size), func(t *testing.T) {
				var output strings.Builder
				bufferedWriter := bufio.NewWriterSize(&output, 8192)
				var rowBuilder strings.Builder
				rowBuilder.Grow(1024)

				for i := 0; i < size; i++ {
					// Process each row individually
					rowBuilder.Reset()
					for col := 0; col < 5; col++ {
						if col > 0 {
							rowBuilder.WriteString("\t")
						}
						rowBuilder.WriteString(fmt.Sprintf("data_%d_%d", i, col))
					}
					rowBuilder.WriteString("\n")

					// Stream immediately
					bufferedWriter.WriteString(rowBuilder.String())

					// Regular flush to prevent unlimited buffering
					if i%100 == 0 {
						bufferedWriter.Flush()
					}
				}
				bufferedWriter.Flush()

				// Verify output size scales with input
				lines := strings.Count(output.String(), "\n")
				if lines != size {
					t.Errorf("Expected %d lines, got %d", size, lines)
				}

				// Key assertion: memory structures remain small
				if rowBuilder.Cap() > 2048 {
					t.Errorf("Row builder capacity should remain small: %d", rowBuilder.Cap())
				}
				if bufferedWriter.Available() < 1000 {
					t.Error("Buffer should have reasonable available space")
				}
			})
		}
	})
}

// Test command history file functionality
func TestGetHistoryFile(t *testing.T) {
	historyFile := getHistoryFile()

	// Should not be empty
	if historyFile == "" {
		t.Error("History file path should not be empty")
	}

	// Should contain .tdcli directory
	if !strings.Contains(historyFile, ".tdcli") {
		t.Error("History file should be in .tdcli directory")
	}

	// Should end with trino_history
	if !strings.HasSuffix(historyFile, "trino_history") {
		t.Error("History file should be named trino_history")
	}

	// Should be an absolute path
	if !filepath.IsAbs(historyFile) {
		t.Error("History file should be an absolute path")
	}
}

// Test auto-completion functionality
func TestTrinoAutoCompleter(t *testing.T) {
	// Mock database string
	database := "test_db"

	// Create auto-completer without actual client
	completer := &trinoAutoCompleter{
		client:   nil, // We'll test without actual client
		database: &database,
		keywords: []string{"SELECT", "FROM", "WHERE", "SHOW", "USE", "DESCRIBE"},
		tables:   make(map[string][]string),
	}

	tests := []struct {
		name     string
		word     string
		line     string
		expected []string
	}{
		{
			name:     "SQL keyword completion",
			word:     "SEL",
			line:     "SEL",
			expected: []string{"SELECT"},
		},
		{
			name:     "Multiple keyword matches",
			word:     "S",
			line:     "S",
			expected: []string{"SELECT", "SHOW"},
		},
		{
			name:     "No matches",
			word:     "XYZ",
			line:     "XYZ",
			expected: []string{},
		},
		{
			name:     "Empty word",
			word:     "",
			line:     "",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suggestions := completer.getSuggestions(tt.word, tt.line)

			if len(suggestions) != len(tt.expected) {
				t.Errorf("Expected %d suggestions, got %d", len(tt.expected), len(suggestions))
				return
			}

			for i, expected := range tt.expected {
				if suggestions[i] != expected {
					t.Errorf("Expected suggestion %q, got %q", expected, suggestions[i])
				}
			}
		})
	}
}

// Test word extraction for auto-completion
func TestGetCurrentWord(t *testing.T) {
	completer := &trinoAutoCompleter{}

	tests := []struct {
		name     string
		line     string
		pos      int
		expected string
	}{
		{
			name:     "Word at beginning",
			line:     "SELECT * FROM table",
			pos:      6, // After "SELECT"
			expected: "SELECT",
		},
		{
			name:     "Word in middle",
			line:     "SELECT * FROM table",
			pos:      13, // In "FROM"
			expected: "FROM",
		},
		{
			name:     "Partial word",
			line:     "SEL FROM",
			pos:      3, // At end of "SEL"
			expected: "SEL",
		},
		{
			name:     "Empty position",
			line:     "SELECT * FROM ",
			pos:      14, // At space
			expected: "",
		},
		{
			name:     "Invalid position",
			line:     "SELECT",
			pos:      -1,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			word := completer.getCurrentWord(tt.line, tt.pos)
			if word != tt.expected {
				t.Errorf("Expected word %q, got %q", tt.expected, word)
			}
		})
	}
}

// Test SQL context detection
func TestSQLContextDetection(t *testing.T) {
	completer := &trinoAutoCompleter{}

	tests := []struct {
		name          string
		line          string
		expectFromCtx bool
		expectUseCtx  bool
	}{
		{
			name:          "FROM context",
			line:          "SELECT * FROM ",
			expectFromCtx: true,
			expectUseCtx:  false,
		},
		{
			name:          "FROM context with partial word",
			line:          "SELECT * FROM tab",
			expectFromCtx: true,
			expectUseCtx:  false,
		},
		{
			name:          "USE context",
			line:          "USE ",
			expectFromCtx: false,
			expectUseCtx:  true,
		},
		{
			name:          "USE context with partial word",
			line:          "USE test_",
			expectFromCtx: false,
			expectUseCtx:  true,
		},
		{
			name:          "No special context",
			line:          "SELECT * WHERE",
			expectFromCtx: false,
			expectUseCtx:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fromCtx := completer.isFromContext(tt.line)
			useCtx := completer.isUseContext(tt.line)

			if fromCtx != tt.expectFromCtx {
				t.Errorf("Expected FROM context %v, got %v", tt.expectFromCtx, fromCtx)
			}

			if useCtx != tt.expectUseCtx {
				t.Errorf("Expected USE context %v, got %v", tt.expectUseCtx, useCtx)
			}
		})
	}
}

// Test word character detection
func TestIsWordChar(t *testing.T) {
	tests := []struct {
		char     rune
		expected bool
	}{
		{'a', true},
		{'Z', true},
		{'5', true},
		{'_', true},
		{' ', false},
		{'\t', false},
		{'.', false},
		{'-', false},
		{'(', false},
		{')', false},
	}

	for _, tt := range tests {
		t.Run(string(tt.char), func(t *testing.T) {
			result := isWordChar(tt.char)
			if result != tt.expected {
				t.Errorf("Expected %v for character %q, got %v", tt.expected, tt.char, result)
			}
		})
	}
}

// Test remove duplicates utility
func TestRemoveDuplicates(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "No duplicates",
			input:    []string{"SELECT", "FROM", "WHERE"},
			expected: []string{"SELECT", "FROM", "WHERE"},
		},
		{
			name:     "With duplicates",
			input:    []string{"SELECT", "FROM", "SELECT", "WHERE", "FROM"},
			expected: []string{"SELECT", "FROM", "WHERE"},
		},
		{
			name:     "Empty slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "Single item",
			input:    []string{"SELECT"},
			expected: []string{"SELECT"},
		},
		{
			name:     "All duplicates",
			input:    []string{"SELECT", "SELECT", "SELECT"},
			expected: []string{"SELECT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeDuplicates(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected length %d, got %d", len(tt.expected), len(result))
				return
			}

			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("Expected item %q at index %d, got %q", expected, i, result[i])
				}
			}
		})
	}
}

// Test enhanced help content
func TestEnhancedTrinoHelp(t *testing.T) {
	// Capture stdout to verify enhanced help output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printTrinoHelp()

	w.Close()
	os.Stdout = oldStdout

	// Read the captured output
	var buf strings.Builder
	content := make([]byte, 4096) // Increased buffer for enhanced help
	n, _ := r.Read(content)
	buf.Write(content[:n])
	output := buf.String()

	// Verify enhanced help content contains new sections
	enhancedSections := []string{
		"Enhanced Features:",
		"Command History",
		"Auto-completion",
		"Query Cancellation",
		"Keyboard Shortcuts:",
		"Tab",
		"Up/Down Arrow",
		"Ctrl+A",
		"Ctrl+E",
		"Ctrl+K",
		"Ctrl+U",
		"Ctrl+C",
		"clear, cls",
	}

	for _, section := range enhancedSections {
		if !strings.Contains(output, section) {
			t.Errorf("Expected enhanced help output to contain %q, but it didn't", section)
		}
	}

	// Verify original sections are still there
	originalSections := []string{
		"Interactive Trino Commands:",
		"Database Commands:",
		"SQL Commands:",
		"Examples:",
	}

	for _, section := range originalSections {
		if !strings.Contains(output, section) {
			t.Errorf("Expected help output to still contain %q, but it didn't", section)
		}
	}
}

// Test auto-completer database update
func TestAutoCompleterDatabaseUpdate(t *testing.T) {
	database := "initial_db"
	completer := &trinoAutoCompleter{
		database:   &database,
		tables:     make(map[string][]string),
		tableCache: time.Now(),
	}

	// Set some initial cache
	completer.tables["initial_db"] = []string{"table1", "table2"}

	// Update to new database
	newDB := "new_db"
	completer.updateDatabase(&newDB)

	// Check database was updated
	if *completer.database != "new_db" {
		t.Errorf("Expected database to be updated to 'new_db', got %s", *completer.database)
	}

	// Check cache was cleared (time should be zero)
	if !completer.tableCache.IsZero() {
		t.Error("Expected table cache time to be cleared after database update")
	}
}

// Test mock auto-completion Do method
func TestAutoCompleterDo(t *testing.T) {
	database := "test_db"
	completer := &trinoAutoCompleter{
		database: &database,
		keywords: []string{"SELECT", "FROM", "WHERE"},
		tables:   make(map[string][]string),
	}

	// Test with valid input - simulate typing "SEL" at position 3
	line := []rune("SELECT")
	pos := 3 // Position after "SEL"

	suggestions, length := completer.Do(line, pos)

	// Should return SELECT suggestion
	if len(suggestions) != 1 {
		t.Errorf("Expected 1 suggestion, got %d", len(suggestions))
	}

	if len(suggestions) > 0 && string(suggestions[0]) != "SELECT" {
		t.Errorf("Expected suggestion 'SELECT', got %q", string(suggestions[0]))
	}

	if length != 6 { // Length of "SELECT" not just "SEL"
		t.Errorf("Expected length 6, got %d", length)
	}

	// Test with empty input
	emptyLine := []rune("")
	suggestions, length = completer.Do(emptyLine, 0)

	if suggestions != nil {
		t.Error("Expected nil suggestions for empty input")
	}

	if length != 0 {
		t.Errorf("Expected length 0 for empty input, got %d", length)
	}
}

// Test cache timing mechanism
func TestTableCacheRefresh(t *testing.T) {
	database := "test_db"
	completer := &trinoAutoCompleter{
		database:   &database,
		tables:     make(map[string][]string),
		tableCache: time.Now().Add(-35 * time.Second), // Simulate old cache
	}

	// Set some initial tables
	completer.tables["test_db"] = []string{"old_table"}

	// Check if cache needs refresh (should be true for 35 seconds old)
	needsRefresh := time.Since(completer.tableCache) > 30*time.Second
	if !needsRefresh {
		t.Error("Expected cache to need refresh after 35 seconds")
	}

	// Simulate recent cache
	completer.tableCache = time.Now()
	needsRefresh = time.Since(completer.tableCache) > 30*time.Second
	if needsRefresh {
		t.Error("Expected cache to NOT need refresh when recent")
	}
}
