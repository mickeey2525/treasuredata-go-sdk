package main

import (
	"encoding/json"
	"fmt"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// printJSON prints any value as formatted JSON
func printJSON(v interface{}) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Printf("Error formatting JSON: %v\n", err)
		return
	}
	fmt.Println(string(data))
}

// formatTDTime formats a TDTime for display
func formatTDTime(t td.TDTime) string {
	if t.Time.IsZero() {
		return "-"
	}
	return t.Time.Format("2006-01-02 15:04:05")
}

// writeOutput writes output to file if specified, otherwise to stdout
func writeOutput(content string, filename string) error {
	if filename == "" {
		fmt.Print(content)
		return nil
	}

	return os.WriteFile(filename, []byte(content), 0644)
}

// formatAndWriteOutput formats data according to the specified format and writes it
func formatAndWriteOutput(data interface{}, format, outputFile string, csvHeader string, csvFormatter func(interface{}) string, tableFormatter func(interface{}) string) error {
	var output string
	
	switch format {
	case "json":
		jsonData, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to format JSON: %v", err)
		}
		output = string(jsonData)
	case "csv":
		output = csvHeader + "\n" + csvFormatter(data)
	default:
		output = tableFormatter(data)
	}
	
	return writeOutput(output, outputFile)
}
