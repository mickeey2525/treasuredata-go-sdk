package cdp

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Flags contains command line flags
type Flags struct {
	APIKey             string
	Region             string
	Format             string
	Output             string
	Verbose            bool
	Database           string
	Status             string
	Priority           int
	Limit              int
	WithDetails        bool
	Query              string
	Folder             string
	Name               string
	Description        string
	Engine             string
	InsecureSkipVerify bool
	CertFile           string
	KeyFile            string
	CAFile             string
}

// wrapError returns a formatted error suitable for propagating up to the
// caller. When verbose, it includes status/message details for TD API errors.
func wrapError(err error, message string, verbose bool) error {
	if err == nil {
		return nil
	}
	if verbose {
		var tdErr *td.ErrorResponse
		if errors.As(err, &tdErr) && tdErr.Response != nil {
			return fmt.Errorf("%s: %w (status: %d, message: %s)", message, err, tdErr.Response.StatusCode, tdErr.Message)
		}
	}
	return fmt.Errorf("%s: %w", message, err)
}

// usageError builds a usage-failure error for a missing/invalid arg.
func usageError(message string) error {
	return errors.New(message)
}

// printJSON encodes data as indented JSON to stdout.
func printJSON(data interface{}) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}
	return nil
}

// FormatOutput formats and outputs data using JSON by default.
func FormatOutput(data interface{}, format, output string) error {
	switch format {
	case "json":
		return printJSON(data)
	default:
		return printJSON(data)
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
