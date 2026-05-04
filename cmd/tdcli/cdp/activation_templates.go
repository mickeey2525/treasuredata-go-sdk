package cdp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleActivationTemplateList handles listing activation templates by parent segment
func HandleActivationTemplateList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Parent Segment ID is required")
	}

	templates, err := client.CDP.ListActivationTemplatesByParentSegment(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to list activation templates", flags.Verbose)
	}

	return FormatOutput(templates, flags.Format, flags.Output)
}

// HandleActivationTemplateCreate handles activation template creation
func HandleActivationTemplateCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Request file is required")
	}

	requestData, err := os.ReadFile(args[0])
	if err != nil {
		return usageError(fmt.Sprintf("Error reading request file: %v", err))
	}

	var request td.CDPActivationTemplateRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return usageError(fmt.Sprintf("Error parsing request JSON: %v", err))
	}

	template, err := client.CDP.CreateActivationTemplate(ctx, &request)
	if err != nil {
		return wrapError(err, "failed to create activation template", flags.Verbose)
	}

	return FormatOutput(template, flags.Format, flags.Output)
}

// HandleActivationTemplateGet handles getting activation template details
func HandleActivationTemplateGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Template ID is required")
	}

	template, err := client.CDP.GetActivationTemplate(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to get activation template", flags.Verbose)
	}

	return FormatOutput(template, flags.Format, flags.Output)
}

// HandleActivationTemplateUpdate handles activation template updates
func HandleActivationTemplateUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Template ID and request file are required")
	}

	requestData, err := os.ReadFile(args[1])
	if err != nil {
		return usageError(fmt.Sprintf("Error reading request file: %v", err))
	}

	var request td.CDPActivationTemplateRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return usageError(fmt.Sprintf("Error parsing request JSON: %v", err))
	}

	template, err := client.CDP.UpdateActivationTemplate(ctx, args[0], &request)
	if err != nil {
		return wrapError(err, "failed to update activation template", flags.Verbose)
	}

	return FormatOutput(template, flags.Format, flags.Output)
}

// HandleActivationTemplateDelete handles activation template deletion
func HandleActivationTemplateDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Template ID is required")
	}

	if err := client.CDP.DeleteActivationTemplate(ctx, args[0]); err != nil {
		return wrapError(err, "failed to delete activation template", flags.Verbose)
	}

	fmt.Printf("Activation template %s deleted successfully\n", args[0])
	return nil
}
