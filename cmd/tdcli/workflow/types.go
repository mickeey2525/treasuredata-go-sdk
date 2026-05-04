package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	HandleWorkflowInit(ctx.Context, []string{w.ProjectName}, ctx.GlobalFlags)
	return nil
}

// Helper functions for consistent output
func PrintJSON(v interface{}) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}
}

// CaptureErrors signals HandleError to panic instead of calling log.Fatalf.
// Set by the main package's runHandlerWithErrorCapture when wrapping commands.
var CaptureErrors = false

func HandleError(err error, message string, verbose bool) {
	if err != nil {
		if CaptureErrors {
			if verbose {
				panic(fmt.Errorf("%s: %v", message, err))
			}
			panic(err)
		}
	}
	if verbose {
		if tdErr, ok := err.(*td.ErrorResponse); ok {
			log.Printf("API Error: %s\n", tdErr.Error())
			if tdErr.Response != nil {
				log.Printf("Status: %s\n", tdErr.Response.Status)
			}
		}
	}
	log.Fatalf("%s: %v", message, err)
}
