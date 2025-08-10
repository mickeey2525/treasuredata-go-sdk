package cdp

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Flags contains command line flags
type Flags struct {
	APIKey      string
	Region      string
	Format      string
	Output      string
	Verbose     bool
	Database    string
	Status      string
	Priority    int
	Limit       int
	WithDetails bool
	Query       string
	Folder      string
	Name        string
	Description string
}

// handleError handles errors with optional verbose output
func handleError(err error, message string, verbose bool) {
	if verbose {
		if tdErr, ok := err.(*td.ErrorResponse); ok {
			log.Fatalf("%s: %v (Status: %d, Message: %s)", message, err, tdErr.Response.StatusCode, tdErr.Message)
		}
	}
	log.Fatalf("%s: %v", message, err)
}

// HandleError is an exported version of handleError for use in other files
func HandleError(err error, verbose bool) {
	handleError(err, "Operation failed", verbose)
}

// handleUsageError handles usage errors with consistent formatting
func handleUsageError(usageMessage string, verbose bool) {
	if verbose {
		log.Fatalf("Usage error: %s", usageMessage)
	}
	log.Fatalf("%s", usageMessage)
}

// FormatOutput formats and outputs data using JSON by default
func FormatOutput(data interface{}, format, output string) {
	switch format {
	case "json":
		printJSON(data)
	default:
		printJSON(data) // Default to JSON for new commands
	}
}

// printJSON prints data as JSON
func printJSON(data interface{}) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		log.Fatalf("Failed to encode JSON: %v", err)
	}
}

// formatAndWriteOutput formats and writes output based on format flag
func formatAndWriteOutput(data interface{}, format, outputFile, csvHeader string, csvFormatter, tableFormatter func(interface{}) string) error {
	var output string

	switch format {
	case "json":
		jsonData, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		output = string(jsonData)
	case "csv":
		output = csvHeader + "\n" + csvFormatter(data)
	default: // table
		output = tableFormatter(data)
	}

	if outputFile != "" {
		return os.WriteFile(outputFile, []byte(output), 0644)
	}

	fmt.Print(output)
	return nil
}
