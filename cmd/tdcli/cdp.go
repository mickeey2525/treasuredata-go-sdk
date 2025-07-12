package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// CDP segment handlers
func handleCDPSegmentCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Usage: cdp segment create <audience-id> <name> <description> <query>")
	}

	segment, err := client.CDP.CreateSegment(ctx, args[0], args[1], args[2], args[3])
	if err != nil {
		handleError(err, "Failed to create segment", flags.Verbose)
	}

	fmt.Printf("Segment created successfully\n")
	fmt.Printf("ID: %s\n", segment.ID)
	fmt.Printf("Name: %s\n", segment.Name)
}

func handleCDPSegmentList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Usage: cdp segment list <audience-id>")
	}

	opts := &td.CDPSegmentListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.CDP.ListSegments(ctx, args[0], opts)
	if err != nil {
		handleError(err, "Failed to list segments", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPSegmentListResponse)
		var csvBuilder strings.Builder
		for _, segment := range resp.Segments {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d,%s,%s\n",
				segment.ID, segment.Name, segment.ProfileCount,
				segment.CreatedAt.Format("2006-01-02 15:04:05"),
				segment.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPSegmentListResponse)
		if len(resp.Segments) == 0 {
			return "No segments found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPROFILES\tCREATED")
		for _, segment := range resp.Segments {
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\n",
				segment.ID, segment.Name, segment.ProfileCount,
				segment.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d segments\n", resp.Total))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "id,name,profile_count,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

func handleCDPSegmentGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment get <audience-id> <segment-id>")
	}

	segment, err := client.CDP.GetSegment(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get segment", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(segment)
	case "csv":
		fmt.Println("id,name,profile_count,created_at,updated_at")
		fmt.Printf("%s,%s,%d,%s,%s\n",
			segment.ID, segment.Name, segment.ProfileCount,
			segment.CreatedAt.Format("2006-01-02 15:04:05"),
			segment.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", segment.ID)
		fmt.Printf("Name: %s\n", segment.Name)
		fmt.Printf("Description: %s\n", segment.Description)
		fmt.Printf("Profile Count: %d\n", segment.ProfileCount)
		fmt.Printf("Created: %s\n", segment.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", segment.UpdatedAt.Format("2006-01-02 15:04:05"))
		if segment.Query != "" {
			fmt.Printf("\nQuery:\n%s\n", segment.Query)
		}
	}
}

func handleCDPSegmentUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp segment update <audience-id> <segment-id> <key=value>...")
	}

	audienceID := args[0]
	segmentID := args[1]
	updates := make(map[string]string)

	// Parse key=value pairs
	for _, arg := range args[2:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		updates[parts[0]] = parts[1]
	}

	segment, err := client.CDP.UpdateSegment(ctx, audienceID, segmentID, updates)
	if err != nil {
		handleError(err, "Failed to update segment", flags.Verbose)
	}

	fmt.Printf("Segment %s updated successfully\n", segment.ID)
}

func handleCDPSegmentDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment delete <audience-id> <segment-id>")
	}

	err := client.CDP.DeleteSegment(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to delete segment", flags.Verbose)
	}

	fmt.Printf("Segment %s deleted successfully\n", args[0])
}

// CDP audience handlers
func handleCDPAudienceCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Name, description, parent database name, and parent table name required")
	}

	audience, err := client.CDP.CreateAudience(ctx, args[0], args[1], args[2], args[3])
	if err != nil {
		handleError(err, "Failed to create audience", flags.Verbose)
	}

	fmt.Printf("Audience created successfully\n")
	fmt.Printf("ID: %s\n", audience.ID)
	fmt.Printf("Name: %s\n", audience.Name)
}

func handleCDPAudienceList(ctx context.Context, client *td.Client, flags Flags) {
	resp, err := client.CDP.ListAudiences(ctx)
	if err != nil {
		handleError(err, "Failed to list audiences", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPAudienceListResponse)
		var csvBuilder strings.Builder
		for _, audience := range resp.Audiences {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d,%s,%s,%s\n",
				audience.ID, audience.Name, audience.Population, audience.ScheduleType,
				audience.CreatedAt.Format("2006-01-02 15:04:05"),
				audience.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPAudienceListResponse)
		if len(resp.Audiences) == 0 {
			return "No audiences found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPOPULATION\tSCHEDULE\tCREATED")
		for _, audience := range resp.Audiences {
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\n",
				audience.ID, audience.Name, audience.Population, audience.ScheduleType,
				audience.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d audiences\n", resp.Total))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "id,name,population,schedule_type,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

func handleCDPAudienceGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	audience, err := client.CDP.GetAudience(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get audience", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(audience)
	case "csv":
		fmt.Println("id,name,population,schedule_type,created_at,updated_at")
		fmt.Printf("%s,%s,%d,%s,%s,%s\n",
			audience.ID, audience.Name, audience.Population, audience.ScheduleType,
			audience.CreatedAt.Format("2006-01-02 15:04:05"),
			audience.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", audience.ID)
		fmt.Printf("Name: %s\n", audience.Name)
		fmt.Printf("Description: %s\n", audience.Description)
		fmt.Printf("Population: %d\n", audience.Population)
		fmt.Printf("Schedule Type: %s\n", audience.ScheduleType)
		if audience.ScheduleOption != nil {
			fmt.Printf("Schedule Option: %s\n", *audience.ScheduleOption)
		}
		fmt.Printf("Timezone: %s\n", audience.Timezone)
		fmt.Printf("Created: %s\n", audience.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", audience.UpdatedAt.Format("2006-01-02 15:04:05"))
		if len(audience.Attributes) > 0 {
			fmt.Printf("\nAttributes (%d):\n", len(audience.Attributes))
			for _, attr := range audience.Attributes {
				fmt.Printf("  - %s (%s)\n", attr.Name, attr.Type)
			}
		}
		if len(audience.Behaviors) > 0 {
			fmt.Printf("\nBehaviors (%d):\n", len(audience.Behaviors))
			for _, behavior := range audience.Behaviors {
				fmt.Printf("  - %s\n", behavior.Name)
			}
		}
	}
}

func handleCDPAudienceDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	err := client.CDP.DeleteAudience(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to delete audience", flags.Verbose)
	}

	fmt.Printf("Audience %s deleted successfully\n", args[0])
}

// CDP activation handlers
func handleCDPActivationCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Segment ID, name, description, and configuration (JSON) required")
	}

	var config map[string]interface{}
	if args[3] != "" {
		err := json.Unmarshal([]byte(args[3]), &config)
		if err != nil {
			log.Fatalf("Invalid configuration JSON: %v", err)
		}
	}

	activation, err := client.CDP.CreateActivation(ctx, args[0], args[1], args[2], config)
	if err != nil {
		handleError(err, "Failed to create activation", flags.Verbose)
	}

	fmt.Printf("Activation created successfully\n")
	fmt.Printf("ID: %s\n", activation.ID)
	fmt.Printf("Name: %s\n", activation.Name)
	fmt.Printf("Status: %s\n", activation.Status)
}

func handleCDPActivationListWithForce(ctx context.Context, client *td.Client, flags Flags, force bool) {
	fmt.Println("⚠️  Warning: 'cdp activations ls' lists activations from ALL audiences.")
	fmt.Println("    For better performance, use specific commands:")
	fmt.Println("    • cdp activations list-by-audience <audience-id>        - List activations for specific audience")
	fmt.Println("    • cdp activations list-by-segment-folder <folder-id>   - List activations for specific folder")
	fmt.Println("    • cdp activations list-by-parent-segment <segment-id>  - List activations for specific segment")
	fmt.Println("    • cdp audience ls                                      - List available audiences first")
	fmt.Println()

	// First get a list of audiences to show activations from all audiences
	audiences, err := client.CDP.ListAudiences(ctx)
	if err != nil {
		handleError(err, "Failed to list audiences", flags.Verbose)
	}

	if len(audiences.Audiences) == 0 {
		fmt.Println("No audiences found")
		return
	}

	fmt.Printf("Found %d audiences. This will make %d API calls to collect all activations.\n", len(audiences.Audiences), len(audiences.Audiences))

	if !force {
		fmt.Print("Do you want to continue? This may take a while and put load on the API server. [y/N]: ")

		var response string
		fmt.Scanln(&response)
		response = strings.ToLower(strings.TrimSpace(response))

		if response != "y" && response != "yes" {
			fmt.Println("Operation cancelled.")
			return
		}
	} else {
		fmt.Println("Force flag enabled, skipping confirmation...")
	}

	fmt.Printf("Collecting activations from %d audiences...\n", len(audiences.Audiences))

	// Collect activations from all audiences
	var allActivations []td.CDPActivation
	total := len(audiences.Audiences)
	for i, audience := range audiences.Audiences {
		if i%10 == 0 || i == total-1 {
			fmt.Printf("Progress: %d/%d audiences processed...\n", i+1, total)
		}

		resp, err := client.CDP.ListActivations(ctx, audience.ID, nil)
		if err != nil {
			// Skip this audience if there's an error, but continue with others
			if flags.Verbose {
				fmt.Printf("Warning: Failed to get activations for audience %s: %v\n", audience.ID, err)
			}
			continue
		}
		allActivations = append(allActivations, resp.Activations...)
	}

	fmt.Printf("Completed! Collected %d total activations from %d audiences.\n", len(allActivations), total)

	// Create a response with all collected activations
	resp := &td.CDPActivationListResponse{
		Activations: allActivations,
		Total:       int64(len(allActivations)),
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,type,audience_id,status,created_at,updated_at")
		for _, activation := range resp.Activations {
			fmt.Printf("%s,%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.AudienceID, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Activations) == 0 {
			fmt.Println("No activations found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.Status, activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d activations\n", resp.Total)
	}
}

func handleCDPActivationGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp activation get <audience-id> <segment-id> <activation-id>")
	}

	activation, err := client.CDP.GetActivation(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to get activation", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(activation)
	case "csv":
		fmt.Println("id,name,type,audience_id,status,created_at,updated_at")
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s\n",
			activation.ID, activation.Name, activation.Type,
			activation.AudienceID, activation.Status,
			activation.CreatedAt.Format("2006-01-02 15:04:05"),
			activation.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", activation.ID)
		fmt.Printf("Name: %s\n", activation.Name)
		fmt.Printf("Type: %s\n", activation.Type)
		fmt.Printf("Audience ID: %s\n", activation.AudienceID)
		fmt.Printf("Status: %s\n", activation.Status)
		fmt.Printf("Created: %s\n", activation.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", activation.UpdatedAt.Format("2006-01-02 15:04:05"))
		if len(activation.Configuration) > 0 {
			fmt.Printf("\nConfiguration:\n")
			configJSON, _ := json.MarshalIndent(activation.Configuration, "  ", "  ")
			fmt.Printf("  %s\n", configJSON)
		}
	}
}

func handleCDPActivationUpdateStatus(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Usage: cdp activation update-status <audience-id> <segment-id> <activation-id> <status>")
	}

	activation, err := client.CDP.UpdateActivationStatus(ctx, args[0], args[1], args[2], args[3])
	if err != nil {
		handleError(err, "Failed to update activation status", flags.Verbose)
	}

	fmt.Printf("Activation %s status updated to %s\n", activation.ID, activation.Status)
}

func handleCDPActivationDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp activation delete <audience-id> <segment-id> <activation-id>")
	}

	err := client.CDP.DeleteActivation(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to delete activation", flags.Verbose)
	}

	fmt.Printf("Activation %s deleted successfully\n", args[0])
}

// CDP audience behavior handlers
func handleCDPAudienceBehaviors(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	behaviors, err := client.CDP.GetAudienceBehaviors(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get audience behaviors", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(behaviors)
	case "csv":
		fmt.Println("id,name,parent_database_name,parent_table_name,parent_key,foreign_key")
		for _, behavior := range behaviors {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				behavior.ID, behavior.Name, behavior.ParentDatabaseName,
				behavior.ParentTableName, behavior.ParentKey, behavior.ForeignKey)
		}
	default:
		if len(behaviors) == 0 {
			fmt.Println("No behaviors found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tDATABASE\tTABLE\tPARENT_KEY\tFOREIGN_KEY")
		for _, behavior := range behaviors {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				behavior.ID, behavior.Name, behavior.ParentDatabaseName,
				behavior.ParentTableName, behavior.ParentKey, behavior.ForeignKey)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d behaviors\n", len(behaviors))
	}
}

func handleCDPAudienceRun(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	execution, err := client.CDP.RunAudience(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to run audience", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(execution)
	case "csv":
		fmt.Println("workflow_id,workflow_session_id,workflow_attempt_id,status,created_at")
		fmt.Printf("%s,%s,%s,%s,%s\n",
			execution.WorkflowID, execution.WorkflowSessionID, execution.WorkflowAttemptID,
			execution.Status, execution.CreatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("Audience execution started\n")
		fmt.Printf("Workflow ID: %s\n", execution.WorkflowID)
		fmt.Printf("Session ID: %s\n", execution.WorkflowSessionID)
		fmt.Printf("Attempt ID: %s\n", execution.WorkflowAttemptID)
		fmt.Printf("Status: %s\n", execution.Status)
		fmt.Printf("Started: %s\n", execution.CreatedAt.Format("2006-01-02 15:04:05"))
	}
}

func handleCDPAudienceExecutions(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	executions, err := client.CDP.GetAudienceExecutions(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get audience executions", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(executions)
	case "csv":
		fmt.Println("workflow_id,workflow_session_id,workflow_attempt_id,status,created_at,finished_at")
		for _, execution := range executions {
			finishedAt := ""
			if execution.FinishedAt != nil {
				finishedAt = execution.FinishedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				execution.WorkflowID, execution.WorkflowSessionID, execution.WorkflowAttemptID,
				execution.Status, execution.CreatedAt.Format("2006-01-02 15:04:05"), finishedAt)
		}
	default:
		if len(executions) == 0 {
			fmt.Println("No executions found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "WORKFLOW_ID\tSESSION_ID\tATTEMPT_ID\tSTATUS\tCREATED\tFINISHED")
		for _, execution := range executions {
			finishedAt := ""
			if execution.FinishedAt != nil {
				finishedAt = execution.FinishedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				execution.WorkflowID, execution.WorkflowSessionID, execution.WorkflowAttemptID,
				execution.Status, execution.CreatedAt.Format("2006-01-02 15:04:05"), finishedAt)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d executions\n", len(executions))
	}
}

func handleCDPAudienceStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	statistics, err := client.CDP.GetAudienceStatistics(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get audience statistics", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(statistics)
	case "csv":
		fmt.Println("timestamp,population,has_data")
		for _, point := range statistics {
			if len(point) >= 3 {
				fmt.Printf("%v,%v,%v\n", point[0], point[1], point[2])
			}
		}
	default:
		if len(statistics) == 0 {
			fmt.Println("No statistics found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "TIMESTAMP\tPOPULATION\tHAS_DATA")
		for _, point := range statistics {
			if len(point) >= 3 {
				fmt.Fprintf(w, "%v\t%v\t%v\n", point[0], point[1], point[2])
			}
		}
		w.Flush()
		fmt.Printf("\nTotal: %d statistics points\n", len(statistics))
	}
}

func handleCDPAudienceSampleValues(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Audience ID and column name required")
	}

	sampleValues, err := client.CDP.GetAudienceSampleValues(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get audience sample values", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(sampleValues)
	case "csv":
		fmt.Println("value,frequency")
		for _, sample := range sampleValues {
			if len(sample) >= 2 {
				fmt.Printf("%v,%v\n", sample[0], sample[1])
			}
		}
	default:
		if len(sampleValues) == 0 {
			fmt.Println("No sample values found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "VALUE\tFREQUENCY")
		for _, sample := range sampleValues {
			if len(sample) >= 2 {
				fmt.Fprintf(w, "%v\t%v\n", sample[0], sample[1])
			}
		}
		w.Flush()
		fmt.Printf("\nTotal: %d sample values\n", len(sampleValues))
	}
}

func handleCDPAudienceBehaviorSamples(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Audience ID, behavior ID, and column name required")
	}

	sampleValues, err := client.CDP.GetAudienceBehaviorSampleValues(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to get audience behavior sample values", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(sampleValues)
	case "csv":
		fmt.Println("value,frequency")
		for _, sample := range sampleValues {
			if len(sample) >= 2 {
				fmt.Printf("%v,%v\n", sample[0], sample[1])
			}
		}
	default:
		if len(sampleValues) == 0 {
			fmt.Println("No sample values found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "VALUE\tFREQUENCY")
		for _, sample := range sampleValues {
			if len(sample) >= 2 {
				fmt.Fprintf(w, "%v\t%v\n", sample[0], sample[1])
			}
		}
		w.Flush()
		fmt.Printf("\nTotal: %d sample values\n", len(sampleValues))
	}
}

// CDP segment folder handlers
func handleCDPSegmentFolders(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Folder ID required")
	}

	resp, err := client.CDP.GetSegmentFolders(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get segment folders", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,parent_id,created_at,updated_at")
		for _, folder := range resp.Folders {
			parentID := ""
			if folder.ParentID != nil {
				parentID = *folder.ParentID
			}
			fmt.Printf("%s,%s,%s,%s,%s\n",
				folder.ID, folder.Name, parentID,
				folder.CreatedAt.Format("2006-01-02 15:04:05"),
				folder.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Folders) == 0 {
			fmt.Println("No folders found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPARENT_ID\tCREATED")
		for _, folder := range resp.Folders {
			parentID := ""
			if folder.ParentID != nil {
				parentID = *folder.ParentID
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				folder.ID, folder.Name, parentID,
				folder.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d folders\n", resp.Total)
	}
}

func handleCDPSegmentQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment query <audience-id> <segment-id>")
	}

	query, err := client.CDP.ExecuteSegmentQuery(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to execute segment query", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(query)
	case "csv":
		fmt.Println("id,segment_id,status,created_at,started_at,finished_at")
		startedAt := ""
		if query.StartedAt != nil {
			startedAt = query.StartedAt.Format("2006-01-02 15:04:05")
		}
		finishedAt := ""
		if query.FinishedAt != nil {
			finishedAt = query.FinishedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s\n",
			query.ID, query.SegmentID, query.Status,
			query.CreatedAt.Format("2006-01-02 15:04:05"),
			startedAt, finishedAt)
	default:
		fmt.Printf("Query ID: %s\n", query.ID)
		fmt.Printf("Segment ID: %s\n", query.SegmentID)
		fmt.Printf("Status: %s\n", query.Status)
		fmt.Printf("Created: %s\n", query.CreatedAt.Format("2006-01-02 15:04:05"))
		if query.StartedAt != nil {
			fmt.Printf("Started: %s\n", query.StartedAt.Format("2006-01-02 15:04:05"))
		}
		if query.FinishedAt != nil {
			fmt.Printf("Finished: %s\n", query.FinishedAt.Format("2006-01-02 15:04:05"))
		}
		if query.Error != "" {
			fmt.Printf("Error: %s\n", query.Error)
		}
	}
}

func handleCDPSegmentNewQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp segment new-query <audience-id> <segment-id> <query-text>")
	}

	query, err := client.CDP.CreateSegmentQuery(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to create segment query", flags.Verbose)
	}

	fmt.Printf("Segment query created successfully\n")
	fmt.Printf("Query ID: %s\n", query.ID)
	fmt.Printf("Status: %s\n", query.Status)
}

func handleCDPSegmentQueryStatus(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp segment query-status <audience-id> <segment-id> <query-id>")
	}

	query, err := client.CDP.GetSegmentQueryStatus(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to get segment query status", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(query)
	case "csv":
		fmt.Println("id,segment_id,status,created_at,started_at,finished_at")
		startedAt := ""
		if query.StartedAt != nil {
			startedAt = query.StartedAt.Format("2006-01-02 15:04:05")
		}
		finishedAt := ""
		if query.FinishedAt != nil {
			finishedAt = query.FinishedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s\n",
			query.ID, query.SegmentID, query.Status,
			query.CreatedAt.Format("2006-01-02 15:04:05"),
			startedAt, finishedAt)
	default:
		fmt.Printf("Query ID: %s\n", query.ID)
		fmt.Printf("Segment ID: %s\n", query.SegmentID)
		fmt.Printf("Status: %s\n", query.Status)
		fmt.Printf("Created: %s\n", query.CreatedAt.Format("2006-01-02 15:04:05"))
		if query.StartedAt != nil {
			fmt.Printf("Started: %s\n", query.StartedAt.Format("2006-01-02 15:04:05"))
		}
		if query.FinishedAt != nil {
			fmt.Printf("Finished: %s\n", query.FinishedAt.Format("2006-01-02 15:04:05"))
		}
		if query.Error != "" {
			fmt.Printf("Error: %s\n", query.Error)
		}
	}
}

func handleCDPSegmentKillQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp segment kill-query <audience-id> <segment-id> <query-id>")
	}

	err := client.CDP.KillSegmentQuery(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to kill segment query", flags.Verbose)
	}

	fmt.Printf("Segment query %s/%s killed successfully\n", args[0], args[1])
}

func handleCDPSegmentCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment customers <audience-id> <segment-id>")
	}

	opts := &td.CDPSegmentCustomerListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.CDP.GetSegmentCustomers(ctx, args[0], args[1], opts)
	if err != nil {
		handleError(err, "Failed to get segment customers", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,attributes")
		for _, customer := range resp.Customers {
			attrsJSON, _ := json.Marshal(customer.Attributes)
			fmt.Printf("%s,\"%s\"\n", customer.ID, string(attrsJSON))
		}
	default:
		if len(resp.Customers) == 0 {
			fmt.Println("No customers found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tATTRIBUTES")
		for _, customer := range resp.Customers {
			attrsJSON, _ := json.Marshal(customer.Attributes)
			fmt.Fprintf(w, "%s\t%s\n", customer.ID, string(attrsJSON))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d customers\n", resp.Total)
	}
}

func handleCDPSegmentStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment statistics <audience-id> <segment-id>")
	}

	statistics, err := client.CDP.GetSegmentStatistics(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get segment statistics", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(statistics)
	case "csv":
		fmt.Println("segment_id,profile_count,last_updated")
		fmt.Printf("%s,%d,%s\n",
			statistics.SegmentID, statistics.ProfileCount,
			statistics.LastUpdated.Format("2006-01-02 15:04:05"))
		fmt.Println("\nStatistics points:")
		fmt.Println("timestamp,count,has_data")
		for _, point := range statistics.Statistics {
			if len(point) >= 3 {
				fmt.Printf("%v,%v,%v\n", point[0], point[1], point[2])
			}
		}
	default:
		fmt.Printf("Segment ID: %s\n", statistics.SegmentID)
		fmt.Printf("Profile Count: %d\n", statistics.ProfileCount)
		fmt.Printf("Last Updated: %s\n", statistics.LastUpdated.Format("2006-01-02 15:04:05"))
		if len(statistics.Statistics) > 0 {
			fmt.Printf("\nStatistics Points (%d):\n", len(statistics.Statistics))
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "TIMESTAMP\tCOUNT\tHAS_DATA")
			for _, point := range statistics.Statistics {
				if len(point) >= 3 {
					fmt.Fprintf(w, "%v\t%v\t%v\n", point[0], point[1], point[2])
				}
			}
			w.Flush()
		}
	}
}

// CDP Folder handlers
func handleCDPCreateAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Audience ID and folder name required")
	}

	req := &td.CDPAudienceFolderCreateRequest{
		Name: args[1],
	}
	if len(args) > 2 && args[2] != "" {
		req.Description = args[2]
	}
	if len(args) > 3 && args[3] != "" {
		req.ParentID = &args[3]
	}

	folder, err := client.CDP.CreateAudienceFolder(ctx, args[0], req)
	if err != nil {
		handleError(err, "Failed to create audience folder", flags.Verbose)
	}

	fmt.Printf("Audience folder created successfully\n")
	fmt.Printf("ID: %s\n", folder.ID)
	fmt.Printf("Name: %s\n", folder.Name)
}

func handleCDPGetAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Audience ID and folder ID required")
	}

	folder, err := client.CDP.GetAudienceFolder(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get audience folder", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(folder)
	case "csv":
		fmt.Println("id,audience_id,name,description,parent_id,created_at,updated_at")
		parentID := ""
		if folder.ParentFolderID != nil {
			parentID = *folder.ParentFolderID
		}
		description := ""
		if folder.Description != nil {
			description = *folder.Description
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s\n",
			folder.ID, folder.AudienceID, folder.Name, description, parentID,
			folder.CreatedAt.Format("2006-01-02 15:04:05"),
			folder.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", folder.ID)
		fmt.Printf("Audience ID: %s\n", folder.AudienceID)
		fmt.Printf("Name: %s\n", folder.Name)
		if folder.Description != nil {
			fmt.Printf("Description: %s\n", *folder.Description)
		}
		if folder.ParentFolderID != nil {
			fmt.Printf("Parent ID: %s\n", *folder.ParentFolderID)
		}
		fmt.Printf("Path: %s\n", folder.Path)
		fmt.Printf("Created: %s\n", folder.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", folder.UpdatedAt.Format("2006-01-02 15:04:05"))
	}
}

// CDP Folder handlers
func handleCDPListFolders(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	resp, err := client.CDP.ListFolders(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to list folders", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPAudienceFolderListResponse)
		var csvBuilder strings.Builder
		for _, folder := range resp.Folders {
			parentID := ""
			if folder.ParentFolderID != nil {
				parentID = *folder.ParentFolderID
			}
			description := ""
			if folder.Description != nil {
				description = *folder.Description
			}
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n",
				folder.ID, folder.AudienceID, folder.Name, description, parentID,
				folder.CreatedAt.Format("2006-01-02 15:04:05"),
				folder.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPAudienceFolderListResponse)
		if len(resp.Folders) == 0 {
			return "No folders found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tDESCRIPTION\tPARENT\tCREATED")
		for _, folder := range resp.Folders {
			parentID := ""
			if folder.ParentFolderID != nil {
				parentID = *folder.ParentFolderID
			}
			description := ""
			if folder.Description != nil {
				description = *folder.Description
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				folder.ID, folder.Name, description, parentID,
				folder.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d folders\n", resp.Total))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "id,audience_id,name,description,parent_folder_id,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

func handleCDPCreateEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Folder name required")
	}

	req := &td.CDPFolderCreateRequest{
		Name: args[0],
	}
	if len(args) > 1 && args[1] != "" {
		req.Description = args[1]
	}
	if len(args) > 2 && args[2] != "" {
		req.ParentID = &args[2]
	}

	folder, err := client.CDP.CreateEntityFolder(ctx, req)
	if err != nil {
		handleError(err, "Failed to create entity folder", flags.Verbose)
	}

	fmt.Printf("Entity folder created successfully\n")
	fmt.Printf("ID: %s\n", folder.ID)
	fmt.Printf("Name: %s\n", folder.Name)
}

func handleCDPGetEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Folder ID required")
	}

	folder, err := client.CDP.GetEntityFolder(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get entity folder", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(folder)
	case "csv":
		fmt.Println("id,name,description,parent_id,path,created_at,updated_at")
		parentID := ""
		if folder.ParentID != nil {
			parentID = *folder.ParentID
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s\n",
			folder.ID, folder.Name, folder.Description, parentID, folder.Path,
			folder.CreatedAt.Format("2006-01-02 15:04:05"),
			folder.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", folder.ID)
		fmt.Printf("Name: %s\n", folder.Name)
		fmt.Printf("Description: %s\n", folder.Description)
		if folder.ParentID != nil {
			fmt.Printf("Parent ID: %s\n", *folder.ParentID)
		}
		fmt.Printf("Path: %s\n", folder.Path)
		fmt.Printf("Created: %s\n", folder.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", folder.UpdatedAt.Format("2006-01-02 15:04:05"))
	}
}

func handleCDPUpdateEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Folder ID and updates (key=value) required")
	}

	folderID := args[0]
	req := &td.CDPFolderUpdateRequest{}

	// Parse key=value pairs
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		switch parts[0] {
		case "name":
			req.Name = parts[1]
		case "description":
			req.Description = parts[1]
		case "parent_id":
			req.ParentID = &parts[1]
		default:
			log.Fatalf("Unknown field: %s", parts[0])
		}
	}

	folder, err := client.CDP.UpdateEntityFolder(ctx, folderID, req)
	if err != nil {
		handleError(err, "Failed to update entity folder", flags.Verbose)
	}

	fmt.Printf("Entity folder %s updated successfully\n", folder.ID)
}

func handleCDPDeleteEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Folder ID required")
	}

	err := client.CDP.DeleteEntityFolder(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to delete entity folder", flags.Verbose)
	}

	fmt.Printf("Entity folder %s deleted successfully\n", args[0])
}

func handleCDPGetEntitiesByFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Folder ID required")
	}

	resp, err := client.CDP.GetEntitiesByFolder(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get entities by folder", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,type,name,description,folder_id,created_at,updated_at")
		for _, entity := range resp.Entities {
			folderID := ""
			if entity.FolderID != nil {
				folderID = *entity.FolderID
			}
			fmt.Printf("%s,%s,%s,%s,%s,%s,%s\n",
				entity.ID, entity.Type, entity.Name, entity.Description, folderID,
				entity.CreatedAt.Format("2006-01-02 15:04:05"),
				entity.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Entities) == 0 {
			fmt.Println("No entities found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tTYPE\tNAME\tCREATED")
		for _, entity := range resp.Entities {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				entity.ID, entity.Type, entity.Name,
				entity.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d entities\n", resp.Total)
	}
}

// CDP Syndication handlers
func handleCDPCreateActivation(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Segment ID, name, type, and configuration (JSON) required")
	}

	var config map[string]interface{}
	err := json.Unmarshal([]byte(args[3]), &config)
	if err != nil {
		log.Fatalf("Invalid configuration JSON: %v", err)
	}

	req := &td.CDPActivationCreateRequest{
		Name:          args[1],
		Type:          args[2],
		Configuration: config,
	}
	if len(args) > 4 && args[4] != "" {
		req.Description = args[4]
	}
	if len(args) > 5 && args[5] != "" {
		req.SegmentFolderID = &args[5]
	}
	if len(args) > 6 && args[6] != "" {
		req.AudienceID = &args[6]
	}

	activation, err := client.CDP.CreateActivationWithRequest(ctx, args[0], req)
	if err != nil {
		handleError(err, "Failed to create activation", flags.Verbose)
	}

	fmt.Printf("Activation created successfully\n")
	fmt.Printf("ID: %s\n", activation.ID)
	fmt.Printf("Name: %s\n", activation.Name)
	fmt.Printf("Type: %s\n", activation.Type)
	fmt.Printf("Status: %s\n", activation.Status)
}

func handleCDPListActivations(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Usage: cdp activations ls <audience-id>")
	}

	opts := &td.CDPActivationListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.CDP.ListActivations(ctx, args[0], opts)
	if err != nil {
		handleError(err, "Failed to list activations", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,type,status,created_at,updated_at")
		for _, activation := range resp.Activations {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Activations) == 0 {
			fmt.Println("No activations found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d activations\n", resp.Total)
	}
}

func handleCDPGetActivation(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp get-activation <audience-id> <segment-id> <syndication-id>")
	}

	activation, err := client.CDP.GetActivation(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to get activation", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(activation)
	case "csv":
		fmt.Println("id,name,type,status,created_at,updated_at")
		fmt.Printf("%s,%s,%s,%s,%s,%s\n",
			activation.ID, activation.Name, activation.Type, activation.Status,
			activation.CreatedAt.Format("2006-01-02 15:04:05"),
			activation.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", activation.ID)
		fmt.Printf("Name: %s\n", activation.Name)
		fmt.Printf("Type: %s\n", activation.Type)
		fmt.Printf("Status: %s\n", activation.Status)
		fmt.Printf("Description: %s\n", "")
		fmt.Printf("Audience ID: %s\n", activation.AudienceID)
		fmt.Printf("Created: %s\n", activation.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", activation.UpdatedAt.Format("2006-01-02 15:04:05"))
		if len(activation.Configuration) > 0 {
			fmt.Printf("\nConfiguration:\n")
			configJSON, _ := json.MarshalIndent(activation.Configuration, "  ", "  ")
			fmt.Printf("  %s\n", configJSON)
		}
	}
}

func handleCDPUpdateActivation(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Usage: cdp update-activation <audience-id> <segment-id> <syndication-id> <key=value>...")
	}

	audienceID := args[0]
	segmentID := args[1]
	activationID := args[2]
	req := &td.CDPActivationUpdateRequest{}

	// Parse key=value pairs
	for _, arg := range args[3:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		switch parts[0] {
		case "name":
			req.Name = parts[1]
		case "description":
			req.Description = parts[1]
		case "status":
			req.Status = parts[1]
		case "configuration":
			var config map[string]interface{}
			err := json.Unmarshal([]byte(parts[1]), &config)
			if err != nil {
				log.Fatalf("Invalid configuration JSON: %v", err)
			}
			req.Configuration = config
		default:
			log.Fatalf("Unknown field: %s", parts[0])
		}
	}

	activation, err := client.CDP.UpdateActivation(ctx, audienceID, segmentID, activationID, req)
	if err != nil {
		handleError(err, "Failed to update activation", flags.Verbose)
	}

	fmt.Printf("Activation %s updated successfully\n", activation.ID)
}

func handleCDPDeleteActivation(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp delete-activation <audience-id> <segment-id> <syndication-id>")
	}

	err := client.CDP.DeleteActivation(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to delete activation", flags.Verbose)
	}

	fmt.Printf("Activation %s deleted successfully\n", args[0])
}

func handleCDPExecuteActivation(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp execute-activation <audience-id> <segment-id> <syndication-id>")
	}

	execution, err := client.CDP.ExecuteActivation(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to execute syndication", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(execution)
	case "csv":
		fmt.Println("id,syndication_id,status,started_at,finished_at,records_exported")
		finishedAt := ""
		if execution.FinishedAt != nil {
			finishedAt = execution.FinishedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%s,%s,%s,%s,%s,%d\n",
			execution.ID, execution.ActivationID, execution.Status,
			execution.StartedAt.Format("2006-01-02 15:04:05"), finishedAt, execution.RecordsExported)
	default:
		fmt.Printf("Syndication execution started\n")
		fmt.Printf("Execution ID: %s\n", execution.ID)
		fmt.Printf("Syndication ID: %s\n", execution.ActivationID)
		fmt.Printf("Status: %s\n", execution.Status)
		fmt.Printf("Started: %s\n", execution.StartedAt.Format("2006-01-02 15:04:05"))
		if execution.FinishedAt != nil {
			fmt.Printf("Finished: %s\n", execution.FinishedAt.Format("2006-01-02 15:04:05"))
		}
		if execution.RecordsExported > 0 {
			fmt.Printf("Records Exported: %d\n", execution.RecordsExported)
		}
		if execution.ErrorMessage != "" {
			fmt.Printf("Error: %s\n", execution.ErrorMessage)
		}
	}
}

func handleCDPGetActivationExecutions(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp get-activation-executions <audience-id> <segment-id> <syndication-id>")
	}

	executions, err := client.CDP.GetActivationExecutions(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to get syndication executions", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(executions)
	case "csv":
		fmt.Println("id,syndication_id,status,started_at,finished_at,records_exported")
		for _, execution := range executions {
			finishedAt := ""
			if execution.FinishedAt != nil {
				finishedAt = execution.FinishedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Printf("%s,%s,%s,%s,%s,%d\n",
				execution.ID, execution.ActivationID, execution.Status,
				execution.StartedAt.Format("2006-01-02 15:04:05"), finishedAt, execution.RecordsExported)
		}
	default:
		if len(executions) == 0 {
			fmt.Println("No executions found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tSTATUS\tSTARTED\tFINISHED\tRECORDS")
		for _, execution := range executions {
			finishedAt := ""
			if execution.FinishedAt != nil {
				finishedAt = execution.FinishedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n",
				execution.ID, execution.Status,
				execution.StartedAt.Format("2006-01-02 15:04:05"), finishedAt, execution.RecordsExported)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d executions\n", len(executions))
	}
}

func handleCDPListActivationsByAudience(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	opts := &td.CDPActivationListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.CDP.GetAudienceActivations(ctx, args[0], opts)
	if err != nil {
		handleError(err, "Failed to get audience syndications", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,type,status,created_at,updated_at")
		for _, activation := range resp.Activations {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Activations) == 0 {
			fmt.Println("No activations found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d activations\n", resp.Total)
	}
}

func handleCDPListActivationsBySegmentFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Segment Folder ID required")
	}

	opts := &td.CDPActivationListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.CDP.GetSegmentFolderActivations(ctx, args[0], opts)
	if err != nil {
		handleError(err, "Failed to get segment folder syndications", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,type,status,created_at,updated_at")
		for _, activation := range resp.Activations {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Activations) == 0 {
			fmt.Println("No activations found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d activations\n", resp.Total)
	}
}

func handleCDPRunActivationForSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Segment ID and Syndication ID required")
	}

	execution, err := client.CDP.RunSegmentActivation(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to run segment syndication", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(execution)
	case "csv":
		fmt.Println("id,syndication_id,status,started_at")
		fmt.Printf("%s,%s,%s,%s\n",
			execution.ID, execution.ActivationID, execution.Status,
			execution.StartedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("Segment syndication execution started\n")
		fmt.Printf("Execution ID: %s\n", execution.ID)
		fmt.Printf("Syndication ID: %s\n", execution.ActivationID)
		fmt.Printf("Status: %s\n", execution.Status)
		fmt.Printf("Started: %s\n", execution.StartedAt.Format("2006-01-02 15:04:05"))
	}
}

func handleCDPListActivationsByParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Parent Segment ID required")
	}

	opts := &td.CDPActivationListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.CDP.GetParentSegmentActivations(ctx, args[0], opts)
	if err != nil {
		handleError(err, "Failed to get parent segment syndications", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,type,status,created_at,updated_at")
		for _, activation := range resp.Activations {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Activations) == 0 {
			fmt.Println("No activations found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d activations\n", resp.Total)
	}
}

func handleCDPGetWorkflowProjectsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Parent Segment ID required")
	}

	resp, err := client.CDP.GetParentSegmentUserDefinedWorkflowProjects(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get parent segment workflow projects", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,description,status,created_at,updated_at")
		for _, project := range resp.Projects {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				project.ID, project.Name, project.Description, project.Status,
				project.CreatedAt.Format("2006-01-02 15:04:05"),
				project.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Projects) == 0 {
			fmt.Println("No workflow projects found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tSTATUS\tCREATED")
		for _, project := range resp.Projects {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				project.ID, project.Name, project.Status,
				project.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d workflow projects\n", resp.Total)
	}
}

func handleCDPGetWorkflowsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Parent Segment ID required")
	}

	resp, err := client.CDP.GetParentSegmentUserDefinedWorkflows(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get parent segment workflows", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,project_id,status,created_at,updated_at")
		for _, workflow := range resp.Workflows {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				workflow.ID, workflow.Name, workflow.ProjectID, workflow.Status,
				workflow.CreatedAt.Format("2006-01-02 15:04:05"),
				workflow.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Workflows) == 0 {
			fmt.Println("No workflows found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPROJECT_ID\tSTATUS\tCREATED")
		for _, workflow := range resp.Workflows {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				workflow.ID, workflow.Name, workflow.ProjectID, workflow.Status,
				workflow.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d workflows\n", resp.Total)
	}
}

func handleCDPGetMatchedActivationsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Parent Segment ID required")
	}

	resp, err := client.CDP.GetParentSegmentMatchedActivations(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get parent segment matched activations", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,type,status,segment_id,parent_segment_id,created_at,updated_at")
		for _, activation := range resp.Activations {
			fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.SegmentID, activation.ParentSegmentID,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Activations) == 0 {
			fmt.Println("No matched activations found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tSEGMENT_ID\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type, activation.Status,
				activation.SegmentID, activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d matched activations\n", resp.Total)
	}
}

// CDP Enhanced Activation handlers
func handleCDPActivationCreateWithStruct(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 5 {
		log.Fatal("Segment ID, name, type, audience ID, and configuration (JSON) required")
	}

	var config map[string]interface{}
	err := json.Unmarshal([]byte(args[4]), &config)
	if err != nil {
		log.Fatalf("Invalid configuration JSON: %v", err)
	}

	req := &td.CDPActivationCreateRequest{
		Name:          args[1],
		Type:          args[2],
		AudienceID:    &args[3],
		Configuration: config,
	}
	if len(args) > 5 && args[5] != "" {
		req.Description = args[5]
	}

	activation, err := client.CDP.CreateActivationWithRequest(ctx, args[0], req)
	if err != nil {
		handleError(err, "Failed to create activation", flags.Verbose)
	}

	fmt.Printf("Activation created successfully\n")
	fmt.Printf("ID: %s\n", activation.ID)
	fmt.Printf("Name: %s\n", activation.Name)
	fmt.Printf("Status: %s\n", activation.Status)
}

func handleCDPActivationUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Usage: cdp activation update <audience-id> <segment-id> <activation-id> <key=value>...")
	}

	audienceID := args[0]
	segmentID := args[1]
	activationID := args[2]
	req := &td.CDPActivationUpdateRequest{}

	// Parse key=value pairs
	for _, arg := range args[3:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		switch parts[0] {
		case "name":
			req.Name = parts[1]
		case "type":
			// Type field not available in update request
		case "description":
			req.Description = parts[1]
		case "status":
			req.Status = parts[1]
		case "configuration":
			var config map[string]interface{}
			err := json.Unmarshal([]byte(parts[1]), &config)
			if err != nil {
				log.Fatalf("Invalid configuration JSON: %v", err)
			}
			req.Configuration = config
		default:
			log.Fatalf("Unknown field: %s", parts[0])
		}
	}

	activation, err := client.CDP.UpdateActivation(ctx, audienceID, segmentID, activationID, req)
	if err != nil {
		handleError(err, "Failed to update activation", flags.Verbose)
	}

	fmt.Printf("Activation %s updated successfully\n", activation.ID)
}

// CDP Token handlers
func handleCDPListTokens(ctx context.Context, client *td.Client, cmd interface{}, flags Flags) {
	// Type assertion to get the command with filters
	var opts *td.CDPTokenListOptions
	var audienceID string
	switch c := cmd.(type) {
	case *CDPTokensListCmd:
		audienceID = c.AudienceID
		opts = &td.CDPTokenListOptions{
			Limit:  c.Limit,
			Offset: c.Offset,
			Type:   c.Type,
			Status: c.Status,
		}
	default:
		opts = &td.CDPTokenListOptions{
			Limit:  100,
			Offset: 0,
		}
	}

	resp, err := client.CDP.ListTokens(ctx, audienceID, opts)
	if err != nil {
		handleError(err, "Failed to list tokens", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPTokenListResponse)
		var csvBuilder strings.Builder
		for _, token := range resp.Tokens {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s\n",
				token.ID, token.Name, token.Type, token.Status,
				token.CreatedAt.Format("2006-01-02 15:04:05"),
				token.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPTokenListResponse)
		if len(resp.Tokens) == 0 {
			return "No tokens found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, token := range resp.Tokens {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				token.ID, token.Name, token.Type, token.Status,
				token.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d tokens\n", resp.Total))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "id,name,type,status,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

func handleCDPGetEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Token ID required")
	}

	token, err := client.CDP.GetEntityToken(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get entity token", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(token)
	case "csv":
		fmt.Println("id,name,type,status,created_at,updated_at")
		fmt.Printf("%s,%s,%s,%s,%s,%s\n",
			token.ID, token.Name, token.Type, token.Status,
			token.CreatedAt.Format("2006-01-02 15:04:05"),
			token.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", token.ID)
		fmt.Printf("Name: %s\n", token.Name)
		fmt.Printf("Type: %s\n", token.Type)
		fmt.Printf("Status: %s\n", token.Status)
		fmt.Printf("Description: %s\n", token.Description)
		if token.ExpiresAt != nil {
			fmt.Printf("Expires At: %s\n", token.ExpiresAt.Format("2006-01-02 15:04:05"))
		}
		if len(token.Scopes) > 0 {
			fmt.Printf("Scopes: %s\n", strings.Join(token.Scopes, ", "))
		}
		fmt.Printf("Created: %s\n", token.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", token.UpdatedAt.Format("2006-01-02 15:04:05"))
		if len(token.Metadata) > 0 {
			fmt.Printf("\nMetadata:\n")
			metadataJSON, _ := json.MarshalIndent(token.Metadata, "  ", "  ")
			fmt.Printf("  %s\n", metadataJSON)
		}
	}
}

func handleCDPUpdateEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Token ID and updates (key=value) required")
	}

	tokenID := args[0]
	req := &td.CDPTokenUpdateRequest{}

	// Parse key=value pairs
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		switch parts[0] {
		case "name":
			req.Name = parts[1]
		case "description":
			req.Description = parts[1]
		case "status":
			req.Status = parts[1]
		case "scopes":
			// Parse comma-separated scopes
			req.Scopes = strings.Split(parts[1], ",")
		case "metadata":
			var metadata map[string]interface{}
			err := json.Unmarshal([]byte(parts[1]), &metadata)
			if err != nil {
				log.Fatalf("Invalid metadata JSON: %v", err)
			}
			req.Metadata = metadata
		default:
			log.Fatalf("Unknown field: %s", parts[0])
		}
	}

	token, err := client.CDP.UpdateEntityToken(ctx, tokenID, req)
	if err != nil {
		handleError(err, "Failed to update entity token", flags.Verbose)
	}

	fmt.Printf("Entity token %s updated successfully\n", token.ID)
}

func handleCDPDeleteEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Token ID required")
	}

	err := client.CDP.DeleteEntityToken(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to delete entity token", flags.Verbose)
	}

	fmt.Printf("Entity token %s deleted successfully\n", args[0])
}
