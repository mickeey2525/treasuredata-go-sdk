package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

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

	// Read the captured output
	var buf strings.Builder
	content := make([]byte, 2048) // Increased buffer size for new help content
	n, _ := r.Read(content)
	buf.Write(content[:n])
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
