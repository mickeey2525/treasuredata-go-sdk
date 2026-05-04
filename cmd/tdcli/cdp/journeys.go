package cdp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleJourneyList handles journey listing by folder
func HandleJourneyList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Folder ID is required")
	}

	journeys, err := client.CDP.ListJourneys(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to list journeys", flags.Verbose)
	}

	return FormatOutput(journeys, flags.Format, flags.Output)
}

// HandleJourneyCreate handles journey creation
func HandleJourneyCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Request file is required")
	}

	requestData, err := os.ReadFile(args[0])
	if err != nil {
		return usageError(fmt.Sprintf("Error reading request file: %v", err))
	}

	var request td.CDPJourneyRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return usageError(fmt.Sprintf("Error parsing request JSON: %v", err))
	}

	journey, err := client.CDP.CreateJourney(ctx, &request)
	if err != nil {
		return wrapError(err, "failed to create journey", flags.Verbose)
	}

	return FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyGet handles getting journey details
func HandleJourneyGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journey, err := client.CDP.GetJourney(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to get journey", flags.Verbose)
	}

	return FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyUpdate handles journey updates
func HandleJourneyUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Journey ID and request file are required")
	}

	requestData, err := os.ReadFile(args[1])
	if err != nil {
		return usageError(fmt.Sprintf("Error reading request file: %v", err))
	}

	var request td.CDPJourneyRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return usageError(fmt.Sprintf("Error parsing request JSON: %v", err))
	}

	journey, err := client.CDP.UpdateJourney(ctx, args[0], &request)
	if err != nil {
		return wrapError(err, "failed to update journey", flags.Verbose)
	}

	return FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyDelete handles journey deletion
func HandleJourneyDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	if err := client.CDP.DeleteJourney(ctx, args[0]); err != nil {
		return wrapError(err, "failed to delete journey", flags.Verbose)
	}

	fmt.Printf("Journey %s deleted successfully\n", args[0])
	return nil
}

// HandleJourneyDetail handles getting journey detail
func HandleJourneyDetail(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journey, err := client.CDP.GetJourneyDetail(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to get journey detail", flags.Verbose)
	}

	return FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyDuplicate handles journey duplication
func HandleJourneyDuplicate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Request file is required")
	}

	requestData, err := os.ReadFile(args[0])
	if err != nil {
		return usageError(fmt.Sprintf("Error reading request file: %v", err))
	}

	var request td.CDPJourneyDuplicateRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return usageError(fmt.Sprintf("Error parsing request JSON: %v", err))
	}

	journey, err := client.CDP.DuplicateJourney(ctx, &request)
	if err != nil {
		return wrapError(err, "failed to duplicate journey", flags.Verbose)
	}

	return FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyPause handles journey pausing
func HandleJourneyPause(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journey, err := client.CDP.PauseJourney(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to pause journey", flags.Verbose)
	}

	return FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyResume handles journey resuming
func HandleJourneyResume(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journey, err := client.CDP.ResumeJourney(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to resume journey", flags.Verbose)
	}

	return FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyStatistics handles getting journey statistics
func HandleJourneyStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journeyID := args[0]
	var from, to *time.Time

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}

		switch args[i] {
		case "--from":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing from date: %v", err))
			}
			from = &t
		case "--to":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing to date: %v", err))
			}
			to = &t
		}
	}

	stats, err := client.CDP.GetJourneyStatistics(ctx, journeyID, from, to)
	if err != nil {
		return wrapError(err, "failed to get journey statistics", flags.Verbose)
	}

	return FormatOutput(stats, flags.Format, flags.Output)
}

// HandleJourneyCustomers handles getting journey customers
func HandleJourneyCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journeyID := args[0]
	var limit, offset *int

	for _, arg := range args[1:] {
		if strings.HasPrefix(arg, "--limit=") {
			val, err := strconv.Atoi(strings.TrimPrefix(arg, "--limit="))
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing limit: %v", err))
			}
			limit = &val
		} else if strings.HasPrefix(arg, "--offset=") {
			val, err := strconv.Atoi(strings.TrimPrefix(arg, "--offset="))
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing offset: %v", err))
			}
			offset = &val
		}
	}

	customers, err := client.CDP.GetJourneyCustomers(ctx, journeyID, limit, offset)
	if err != nil {
		return wrapError(err, "failed to get journey customers", flags.Verbose)
	}

	return FormatOutput(customers, flags.Format, flags.Output)
}

// HandleJourneyStageCustomers handles getting journey stage customers
func HandleJourneyStageCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Journey ID and Stage ID are required")
	}

	journeyID := args[0]
	stageID := args[1]
	var limit, offset *int

	for _, arg := range args[2:] {
		if strings.HasPrefix(arg, "--limit=") {
			val, err := strconv.Atoi(strings.TrimPrefix(arg, "--limit="))
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing limit: %v", err))
			}
			limit = &val
		} else if strings.HasPrefix(arg, "--offset=") {
			val, err := strconv.Atoi(strings.TrimPrefix(arg, "--offset="))
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing offset: %v", err))
			}
			offset = &val
		}
	}

	customers, err := client.CDP.GetJourneyStageCustomers(ctx, journeyID, stageID, limit, offset)
	if err != nil {
		return wrapError(err, "failed to get journey stage customers", flags.Verbose)
	}

	return FormatOutput(customers, flags.Format, flags.Output)
}

// HandleJourneyConversionSankey handles getting journey conversion sankey charts
func HandleJourneyConversionSankey(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journeyID := args[0]
	var from, to *time.Time

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}

		switch args[i] {
		case "--from":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing from date: %v", err))
			}
			from = &t
		case "--to":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing to date: %v", err))
			}
			to = &t
		}
	}

	sankey, err := client.CDP.GetJourneyConversionSankeyCharts(ctx, journeyID, from, to)
	if err != nil {
		return wrapError(err, "failed to get journey conversion sankey", flags.Verbose)
	}

	return FormatOutput(sankey, flags.Format, flags.Output)
}

// HandleJourneyActivationSankey handles getting journey activation sankey charts
func HandleJourneyActivationSankey(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journeyID := args[0]
	var from, to *time.Time

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}

		switch args[i] {
		case "--from":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing from date: %v", err))
			}
			from = &t
		case "--to":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				return usageError(fmt.Sprintf("Error parsing to date: %v", err))
			}
			to = &t
		}
	}

	sankey, err := client.CDP.GetJourneyActivationSankeyCharts(ctx, journeyID, from, to)
	if err != nil {
		return wrapError(err, "failed to get journey activation sankey", flags.Verbose)
	}

	return FormatOutput(sankey, flags.Format, flags.Output)
}

// HandleJourneySegmentRules handles listing journey segment rules
func HandleJourneySegmentRules(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Audience ID is required")
	}

	rules, err := client.CDP.ListJourneySegmentRules(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to list journey segment rules", flags.Verbose)
	}

	return FormatOutput(rules, flags.Format, flags.Output)
}

// HandleJourneyBehaviors handles getting available behaviors for step
func HandleJourneyBehaviors(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journeyID := args[0]
	var stepID *string

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}

		if args[i] == "--step-id" {
			stepID = &args[i+1]
		}
	}

	behaviors, err := client.CDP.GetAvailableBehaviorsForStep(ctx, journeyID, stepID)
	if err != nil {
		return wrapError(err, "failed to get journey behaviors", flags.Verbose)
	}

	return FormatOutput(behaviors, flags.Format, flags.Output)
}

// HandleJourneyTemplates handles getting activation templates for step
func HandleJourneyTemplates(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	journeyID := args[0]
	var stepID *string

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}

		if args[i] == "--step-id" {
			stepID = &args[i+1]
		}
	}

	templates, err := client.CDP.GetActivationTemplatesForStep(ctx, journeyID, stepID)
	if err != nil {
		return wrapError(err, "failed to get journey templates", flags.Verbose)
	}

	return FormatOutput(templates, flags.Format, flags.Output)
}

// HandleJourneyActivationList lists journey activations
func HandleJourneyActivationList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Journey ID is required")
	}

	activations, err := client.CDP.ListJourneyActivations(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to list journey activations", flags.Verbose)
	}

	return FormatOutput(activations, flags.Format, flags.Output)
}

// HandleJourneyActivationCreate creates a journey activation
func HandleJourneyActivationCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Journey ID and request file are required")
	}

	requestData, err := os.ReadFile(args[1])
	if err != nil {
		return usageError(fmt.Sprintf("Error reading request file: %v", err))
	}

	var request td.CDPJourneyActivationRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return usageError(fmt.Sprintf("Error parsing request JSON: %v", err))
	}

	activation, err := client.CDP.CreateJourneyActivation(ctx, args[0], &request)
	if err != nil {
		return wrapError(err, "failed to create journey activation", flags.Verbose)
	}

	return FormatOutput(activation, flags.Format, flags.Output)
}

// HandleJourneyActivationGet gets a journey activation
func HandleJourneyActivationGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Journey ID and Activation Step ID are required")
	}

	activation, err := client.CDP.GetJourneyActivation(ctx, args[0], args[1])
	if err != nil {
		return wrapError(err, "failed to get journey activation", flags.Verbose)
	}

	return FormatOutput(activation, flags.Format, flags.Output)
}

// HandleJourneyActivationUpdate updates a journey activation
func HandleJourneyActivationUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return usageError("Journey ID, Activation Step ID, and request file are required")
	}

	requestData, err := os.ReadFile(args[2])
	if err != nil {
		return usageError(fmt.Sprintf("Error reading request file: %v", err))
	}

	var request td.CDPJourneyActivationRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return usageError(fmt.Sprintf("Error parsing request JSON: %v", err))
	}

	activation, err := client.CDP.UpdateJourneyActivation(ctx, args[0], args[1], &request)
	if err != nil {
		return wrapError(err, "failed to update journey activation", flags.Verbose)
	}

	return FormatOutput(activation, flags.Format, flags.Output)
}
