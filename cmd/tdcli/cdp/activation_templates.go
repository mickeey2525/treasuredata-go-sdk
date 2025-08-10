package cdp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleActivationTemplateList handles listing activation templates by parent segment
func HandleActivationTemplateList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Parent Segment ID is required", flags.Verbose)
	}

	parentSegmentID := args[0]

	templates, err := client.CDP.ListActivationTemplatesByParentSegment(ctx, parentSegmentID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(templates, flags.Format, flags.Output)
}

// HandleActivationTemplateCreate handles activation template creation
func HandleActivationTemplateCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Request file is required", flags.Verbose)
	}

	requestFile := args[0]

	// Read the request file
	requestData, err := os.ReadFile(requestFile)
	if err != nil {
		handleUsageError(fmt.Sprintf("Error reading request file: %v", err), flags.Verbose)
	}

	// Parse the request
	var request td.CDPActivationTemplateRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		handleUsageError(fmt.Sprintf("Error parsing request JSON: %v", err), flags.Verbose)
	}

	template, err := client.CDP.CreateActivationTemplate(ctx, &request)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(template, flags.Format, flags.Output)
}

// HandleActivationTemplateGet handles getting activation template details
func HandleActivationTemplateGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Template ID is required", flags.Verbose)
	}

	templateID := args[0]

	template, err := client.CDP.GetActivationTemplate(ctx, templateID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(template, flags.Format, flags.Output)
}

// HandleActivationTemplateUpdate handles activation template updates
func HandleActivationTemplateUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		handleUsageError("Error: Template ID and request file are required", flags.Verbose)
	}

	templateID := args[0]
	requestFile := args[1]

	// Read the request file
	requestData, err := os.ReadFile(requestFile)
	if err != nil {
		handleUsageError(fmt.Sprintf("Error reading request file: %v", err), flags.Verbose)
	}

	// Parse the request
	var request td.CDPActivationTemplateRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		handleUsageError(fmt.Sprintf("Error parsing request JSON: %v", err), flags.Verbose)
	}

	template, err := client.CDP.UpdateActivationTemplate(ctx, templateID, &request)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(template, flags.Format, flags.Output)
}

// HandleActivationTemplateDelete handles activation template deletion
func HandleActivationTemplateDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Template ID is required", flags.Verbose)
	}

	templateID := args[0]

	err := client.CDP.DeleteActivationTemplate(ctx, templateID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	fmt.Printf("Activation template %s deleted successfully\n", templateID)
}
