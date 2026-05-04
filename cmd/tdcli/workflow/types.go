package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Flags struct for compatibility with existing handlers
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
	Engine             string
	InsecureSkipVerify bool
	CertFile           string
	KeyFile            string
	CAFile             string
}

// CLIContext structure for command execution - matches main CLI
type CLIContext struct {
	Context     context.Context
	Client      *td.Client
	GlobalFlags Flags
}

// WorkflowInitCmd struct for workflow init command
type WorkflowInitCmd struct {
	ProjectName string
}

func (w *WorkflowInitCmd) Run(ctx *CLIContext) error {
	return HandleWorkflowInit(ctx.Context, []string{w.ProjectName}, ctx.GlobalFlags)
}

// PrintJSON encodes the value as indented JSON to stdout.
func PrintJSON(v interface{}) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return nil
}

// wrapError formats an error with context for propagation up the call stack.
// When verbose, it appends TD API status details when available.
func wrapError(err error, message string, verbose bool) error {
	if err == nil {
		return nil
	}
	if verbose {
		var tdErr *td.ErrorResponse
		if errors.As(err, &tdErr) && tdErr.Response != nil {
			return fmt.Errorf("%s: %w (status: %s)", message, err, tdErr.Response.Status)
		}
	}
	return fmt.Errorf("%s: %w", message, err)
}

// usageError builds an error for a missing/invalid argument.
func usageError(message string) error {
	return errors.New(message)
}
