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
func HandleJourneyList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Folder ID is required", flags.Verbose)
	}

	folderID := args[0]

	journeys, err := client.CDP.ListJourneys(ctx, folderID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(journeys, flags.Format, flags.Output)
}

// HandleJourneyCreate handles journey creation
func HandleJourneyCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
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
	var request td.CDPJourneyRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		handleUsageError(fmt.Sprintf("Error parsing request JSON: %v", err), flags.Verbose)
	}

	journey, err := client.CDP.CreateJourney(ctx, &request)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyGet handles getting journey details
func HandleJourneyGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]

	journey, err := client.CDP.GetJourney(ctx, journeyID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyUpdate handles journey updates
func HandleJourneyUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		handleUsageError("Error: Journey ID and request file are required", flags.Verbose)
	}

	journeyID := args[0]
	requestFile := args[1]

	// Read the request file
	requestData, err := os.ReadFile(requestFile)
	if err != nil {
		handleUsageError(fmt.Sprintf("Error reading request file: %v", err), flags.Verbose)
	}

	// Parse the request
	var request td.CDPJourneyRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		handleUsageError(fmt.Sprintf("Error parsing request JSON: %v", err), flags.Verbose)
	}

	journey, err := client.CDP.UpdateJourney(ctx, journeyID, &request)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyDelete handles journey deletion
func HandleJourneyDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]

	err := client.CDP.DeleteJourney(ctx, journeyID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	fmt.Printf("Journey %s deleted successfully\n", journeyID)
}

// HandleJourneyDetail handles getting journey detail
func HandleJourneyDetail(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]

	journey, err := client.CDP.GetJourneyDetail(ctx, journeyID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyDuplicate handles journey duplication
func HandleJourneyDuplicate(ctx context.Context, client *td.Client, args []string, flags Flags) {
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
	var request td.CDPJourneyDuplicateRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		handleUsageError(fmt.Sprintf("Error parsing request JSON: %v", err), flags.Verbose)
	}

	journey, err := client.CDP.DuplicateJourney(ctx, &request)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyPause handles journey pausing
func HandleJourneyPause(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]

	journey, err := client.CDP.PauseJourney(ctx, journeyID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyResume handles journey resuming
func HandleJourneyResume(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]

	journey, err := client.CDP.ResumeJourney(ctx, journeyID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(journey, flags.Format, flags.Output)
}

// HandleJourneyStatistics handles getting journey statistics
func HandleJourneyStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]
	var from, to *time.Time

	// Parse optional date parameters
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}

		switch args[i] {
		case "--from":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing from date: %v", err), flags.Verbose)
			}
			from = &t
		case "--to":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing to date: %v", err), flags.Verbose)
			}
			to = &t
		}
	}

	stats, err := client.CDP.GetJourneyStatistics(ctx, journeyID, from, to)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(stats, flags.Format, flags.Output)
}

// HandleJourneyCustomers handles getting journey customers
func HandleJourneyCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]
	var limit, offset *int

	// Parse optional parameters
	for _, arg := range args[1:] {
		if strings.HasPrefix(arg, "--limit=") {
			val, err := strconv.Atoi(strings.TrimPrefix(arg, "--limit="))
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing limit: %v", err), flags.Verbose)
			}
			limit = &val
		} else if strings.HasPrefix(arg, "--offset=") {
			val, err := strconv.Atoi(strings.TrimPrefix(arg, "--offset="))
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing offset: %v", err), flags.Verbose)
			}
			offset = &val
		}
	}

	customers, err := client.CDP.GetJourneyCustomers(ctx, journeyID, limit, offset)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(customers, flags.Format, flags.Output)
}

// HandleJourneyStageCustomers handles getting journey stage customers
func HandleJourneyStageCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		handleUsageError("Error: Journey ID and Stage ID are required", flags.Verbose)
	}

	journeyID := args[0]
	stageID := args[1]
	var limit, offset *int

	// Parse optional parameters
	for _, arg := range args[2:] {
		if strings.HasPrefix(arg, "--limit=") {
			val, err := strconv.Atoi(strings.TrimPrefix(arg, "--limit="))
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing limit: %v", err), flags.Verbose)
			}
			limit = &val
		} else if strings.HasPrefix(arg, "--offset=") {
			val, err := strconv.Atoi(strings.TrimPrefix(arg, "--offset="))
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing offset: %v", err), flags.Verbose)
			}
			offset = &val
		}
	}

	customers, err := client.CDP.GetJourneyStageCustomers(ctx, journeyID, stageID, limit, offset)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(customers, flags.Format, flags.Output)
}

// HandleJourneyConversionSankey handles getting journey conversion sankey charts
func HandleJourneyConversionSankey(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]
	var from, to *time.Time

	// Parse optional date parameters
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}

		switch args[i] {
		case "--from":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing from date: %v", err), flags.Verbose)
			}
			from = &t
		case "--to":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing to date: %v", err), flags.Verbose)
			}
			to = &t
		}
	}

	sankey, err := client.CDP.GetJourneyConversionSankeyCharts(ctx, journeyID, from, to)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(sankey, flags.Format, flags.Output)
}

// HandleJourneyActivationSankey handles getting journey activation sankey charts
func HandleJourneyActivationSankey(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]
	var from, to *time.Time

	// Parse optional date parameters
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}

		switch args[i] {
		case "--from":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing from date: %v", err), flags.Verbose)
			}
			from = &t
		case "--to":
			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				handleUsageError(fmt.Sprintf("Error parsing to date: %v", err), flags.Verbose)
			}
			to = &t
		}
	}

	sankey, err := client.CDP.GetJourneyActivationSankeyCharts(ctx, journeyID, from, to)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(sankey, flags.Format, flags.Output)
}

// HandleJourneySegmentRules handles listing journey segment rules
func HandleJourneySegmentRules(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Audience ID is required", flags.Verbose)
	}

	audienceID := args[0]

	rules, err := client.CDP.ListJourneySegmentRules(ctx, audienceID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(rules, flags.Format, flags.Output)
}

// HandleJourneyBehaviors handles getting available behaviors for step
func HandleJourneyBehaviors(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]
	var stepID *string

	// Parse optional step ID parameter
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
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(behaviors, flags.Format, flags.Output)
}

// HandleJourneyTemplates handles getting activation templates for step
func HandleJourneyTemplates(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]
	var stepID *string

	// Parse optional step ID parameter
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
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(templates, flags.Format, flags.Output)
}

// Journey Activation handlers
func HandleJourneyActivationList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Error: Journey ID is required", flags.Verbose)
	}

	journeyID := args[0]

	activations, err := client.CDP.ListJourneyActivations(ctx, journeyID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(activations, flags.Format, flags.Output)
}

func HandleJourneyActivationCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		handleUsageError("Error: Journey ID and request file are required", flags.Verbose)
	}

	journeyID := args[0]
	requestFile := args[1]

	// Read the request file
	requestData, err := os.ReadFile(requestFile)
	if err != nil {
		handleUsageError(fmt.Sprintf("Error reading request file: %v", err), flags.Verbose)
	}

	// Parse the request
	var request td.CDPJourneyActivationRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		handleUsageError(fmt.Sprintf("Error parsing request JSON: %v", err), flags.Verbose)
	}

	activation, err := client.CDP.CreateJourneyActivation(ctx, journeyID, &request)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(activation, flags.Format, flags.Output)
}

func HandleJourneyActivationGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		handleUsageError("Error: Journey ID and Activation Step ID are required", flags.Verbose)
	}

	journeyID := args[0]
	activationStepID := args[1]

	activation, err := client.CDP.GetJourneyActivation(ctx, journeyID, activationStepID)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(activation, flags.Format, flags.Output)
}

func HandleJourneyActivationUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		handleUsageError("Error: Journey ID, Activation Step ID, and request file are required", flags.Verbose)
	}

	journeyID := args[0]
	activationStepID := args[1]
	requestFile := args[2]

	// Read the request file
	requestData, err := os.ReadFile(requestFile)
	if err != nil {
		handleUsageError(fmt.Sprintf("Error reading request file: %v", err), flags.Verbose)
	}

	// Parse the request
	var request td.CDPJourneyActivationRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		handleUsageError(fmt.Sprintf("Error parsing request JSON: %v", err), flags.Verbose)
	}

	activation, err := client.CDP.UpdateJourneyActivation(ctx, journeyID, activationStepID, &request)
	if err != nil {
		HandleError(err, flags.Verbose)
		return
	}

	FormatOutput(activation, flags.Format, flags.Output)
}
