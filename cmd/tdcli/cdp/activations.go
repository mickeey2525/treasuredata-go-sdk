package cdp

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

// HandleActivationCreate creates a new CDP activation
func HandleActivationCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleActivationCreateWithStruct creates a new CDP activation using struct-based API
func HandleActivationCreateWithStruct(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Name, type, segment ID, and configuration (JSON) required")
	}

	var config map[string]interface{}
	if args[3] != "" {
		err := json.Unmarshal([]byte(args[3]), &config)
		if err != nil {
			log.Fatalf("Invalid configuration JSON: %v", err)
		}
	}

	req := &td.CDPActivationCreateRequest{
		Name:          args[0],
		Type:          args[1],
		Configuration: config,
	}

	if len(args) > 4 {
		req.Description = args[4]
	}

	activation, err := client.CDP.CreateActivationWithRequest(ctx, args[2], req)
	if err != nil {
		handleError(err, "Failed to create activation with struct", flags.Verbose)
	}

	fmt.Printf("Activation created successfully\n")
	fmt.Printf("ID: %s\n", activation.ID)
	fmt.Printf("Name: %s\n", activation.Name)
	fmt.Printf("Status: %s\n", activation.Status)
}

// HandleActivationListWithForce lists all activations with optional force flag
func HandleActivationListWithForce(ctx context.Context, client *td.Client, flags Flags, force bool) {
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

// HandleActivationGet retrieves a specific activation
func HandleActivationGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp activation get <audience-id> <segment-id> <activation-id>")
	}

	activation, err := client.CDP.GetActivation(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to get activation", flags.Verbose)
	}

	printActivationDetails(activation, flags)
}

// printActivationDetails prints activation details in various formats
func printActivationDetails(activation *td.CDPActivation, flags Flags) {
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
		fmt.Printf("Description: %s\n", activation.Description)
		fmt.Printf("Type: %s\n", activation.Type)
		fmt.Printf("Segment ID: %s\n", activation.SegmentID)
		fmt.Printf("Audience ID: %s\n", activation.AudienceID)
		fmt.Printf("Connection ID: %s\n", activation.ConnectionID)
		fmt.Printf("Schedule Type: %s\n", activation.ScheduleType)
		if activation.ScheduleOption != nil {
			fmt.Printf("Schedule Option: %s\n", *activation.ScheduleOption)
		}
		fmt.Printf("Timezone: %s\n", activation.Timezone)
		fmt.Printf("All Columns: %t\n", activation.AllColumns)
		fmt.Printf("Valid: %t\n", activation.Valid)
		fmt.Printf("Status: %s\n", activation.Status)
		fmt.Printf("Created: %s\n", activation.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", activation.UpdatedAt.Format("2006-01-02 15:04:05"))

		if activation.CreatedBy != nil {
			fmt.Printf("Created By: %s (%s)\n", activation.CreatedBy.Name, activation.CreatedBy.ID)
		}
		if activation.UpdatedBy != nil {
			fmt.Printf("Updated By: %s (%s)\n", activation.UpdatedBy.Name, activation.UpdatedBy.ID)
		}

		if len(activation.NotifyOn) > 0 {
			fmt.Printf("Notify On: %s\n", strings.Join(activation.NotifyOn, ", "))
		}

		if len(activation.EmailRecipients) > 0 {
			var recipients []string
			for _, id := range activation.EmailRecipients {
				recipients = append(recipients, fmt.Sprintf("%d", id))
			}
			fmt.Printf("Email Recipients: %s\n", strings.Join(recipients, ", "))
		}

		if activation.ConnectorConfig != nil {
			configJSON, _ := json.MarshalIndent(activation.ConnectorConfig, "", "  ")
			fmt.Printf("\nConnector Configuration:\n")
			fmt.Printf("  %s\n", configJSON)
		}

		if len(activation.Columns) > 0 {
			fmt.Printf("\nColumns (%d):\n", len(activation.Columns))
			for i, col := range activation.Columns {
				fmt.Printf("  %d. %s -> %s\n", i+1, col.Source.Column, col.Column)
			}
		}

		if len(activation.Executions) > 0 {
			fmt.Printf("\nRecent Executions (%d):\n", len(activation.Executions))
			for i, exec := range activation.Executions {
				status := fmt.Sprintf("ID: %s, Status: %s", exec.ID, exec.Status)
				fmt.Printf("  %d. %s\n", i+1, status)
			}
		}
	}
}

// HandleActivationUpdateStatus updates activation status
func HandleActivationUpdateStatus(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Usage: cdp activation update-status <audience-id> <segment-id> <activation-id> <status>")
	}

	_, err := client.CDP.UpdateActivationStatus(ctx, args[0], args[1], args[2], args[3])
	if err != nil {
		handleError(err, "Failed to update activation status", flags.Verbose)
	}

	fmt.Printf("Activation status updated to '%s' successfully\n", args[3])
}

// HandleActivationUpdate updates an activation
func HandleActivationUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Usage: cdp activation update <audience-id> <segment-id> <activation-id> <updates-json>")
	}

	var updates td.CDPActivationUpdateRequest
	err := json.Unmarshal([]byte(args[3]), &updates)
	if err != nil {
		log.Fatalf("Invalid updates JSON: %v", err)
	}

	activation, err := client.CDP.UpdateActivation(ctx, args[0], args[1], args[2], &updates)
	if err != nil {
		handleError(err, "Failed to update activation", flags.Verbose)
	}

	fmt.Printf("Activation updated successfully\n")
	fmt.Printf("ID: %s\n", activation.ID)
	fmt.Printf("Name: %s\n", activation.Name)
	fmt.Printf("Status: %s\n", activation.Status)
}

// HandleActivationDelete deletes an activation
func HandleActivationDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp activation delete <audience-id> <segment-id> <activation-id>")
	}

	err := client.CDP.DeleteActivation(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to delete activation", flags.Verbose)
	}

	fmt.Printf("Activation %s deleted successfully\n", args[2])
}

// HandleActivationExecute executes an activation
func HandleActivationExecute(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp activation execute <audience-id> <segment-id> <activation-id>")
	}

	execution, err := client.CDP.ExecuteActivation(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to execute activation", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(execution)
	case "csv":
		fmt.Println("id,status,created_at")
		fmt.Printf("%s,%s,%s\n", execution.ID, execution.Status, execution.CreatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("Activation executed successfully\n")
		fmt.Printf("Execution ID: %s\n", execution.ID)
		fmt.Printf("Status: %s\n", execution.Status)
		fmt.Printf("Created: %s\n", execution.CreatedAt.Format("2006-01-02 15:04:05"))
	}
}

// HandleActivationGetExecutions gets activation execution history
func HandleActivationGetExecutions(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp activation executions <audience-id> <segment-id> <activation-id>")
	}

	executions, err := client.CDP.GetActivationExecutions(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to get activation executions", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		executions := data.([]*td.CDPActivationExecution)
		var csvBuilder strings.Builder
		for _, exec := range executions {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s\n",
				exec.ID, exec.Status, exec.CreatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		executions := data.([]*td.CDPActivationExecution)
		if len(executions) == 0 {
			return "No executions found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tSTATUS\tCREATED")
		for _, exec := range executions {
			fmt.Fprintf(w, "%s\t%s\t%s\n",
				exec.ID, exec.Status, exec.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d executions\n", len(executions)))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(executions, flags.Format, flags.Output, "id,status,created_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleActivationListByAudience lists activations for a specific audience
func HandleActivationListByAudience(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	resp, err := client.CDP.ListActivations(ctx, args[0], nil)
	if err != nil {
		// Enhanced error handling for common issues
		if tdErr, ok := err.(*td.ErrorResponse); ok {
			switch tdErr.Response.StatusCode {
			case 422:
				log.Fatalf("Invalid audience ID '%s'. Please use a valid audience ID.\n\nTo find valid audience IDs, run:\n  cdp audiences ls\n\nIf you want activations for a parent segment, use:\n  cdp activations list-by-parent-segment %s", args[0], args[0])
			case 404:
				log.Fatalf("Audience '%s' not found. Please check the audience ID and try again.\n\nTo find valid audience IDs, run:\n  cdp audiences ls", args[0])
			}
		}
		handleError(err, "Failed to list activations", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPActivationListResponse)
		var csvBuilder strings.Builder
		for _, activation := range resp.Activations {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.AudienceID, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPActivationListResponse)
		if len(resp.Activations) == 0 {
			return "No activations found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.Status, activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d activations\n", resp.Total))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "id,name,type,audience_id,status,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleActivationListBySegmentFolder lists activations for a segment folder
func HandleActivationListBySegmentFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Segment folder ID required")
	}

	opts := &td.CDPActivationListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.CDP.GetSegmentFolderActivations(ctx, args[0], opts)
	if err != nil {
		handleError(err, "Failed to list activations for segment folder", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPActivationListResponse)
		var csvBuilder strings.Builder
		for _, activation := range resp.Activations {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.AudienceID, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPActivationListResponse)
		if len(resp.Activations) == 0 {
			return "No activations found for segment folder\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.Status, activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d activations\n", resp.Total))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "id,name,type,audience_id,status,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleActivationRunForSegment runs activation for a segment
func HandleActivationRunForSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp activation run-segment <segment-id> <activation-id>")
	}

	execution, err := client.CDP.RunSegmentActivation(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to run activation for segment", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(execution)
	case "csv":
		fmt.Println("id,status,created_at")
		fmt.Printf("%s,%s,%s\n", execution.ID, execution.Status, execution.CreatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("Segment activation executed successfully\n")
		fmt.Printf("Execution ID: %s\n", execution.ID)
		fmt.Printf("Status: %s\n", execution.Status)
		fmt.Printf("Created: %s\n", execution.CreatedAt.Format("2006-01-02 15:04:05"))
	}
}

// HandleActivationListByParentSegment lists activations for a parent segment
func HandleActivationListByParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Parent segment ID required")
	}

	opts := &td.CDPActivationListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.CDP.GetParentSegmentActivations(ctx, args[0], opts)
	if err != nil {
		handleError(err, "Failed to list activations for parent segment", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPActivationListResponse)
		var csvBuilder strings.Builder
		for _, activation := range resp.Activations {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.AudienceID, activation.Status,
				activation.CreatedAt.Format("2006-01-02 15:04:05"),
				activation.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPActivationListResponse)
		if len(resp.Activations) == 0 {
			return "No activations found for parent segment\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.Status, activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d activations\n", resp.Total))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "id,name,type,audience_id,status,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleGetMatchedActivationsForParentSegment gets matched activations for a parent segment
func HandleGetMatchedActivationsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Parent segment ID required")
	}

	resp, err := client.CDP.GetParentSegmentMatchedActivations(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get matched activations for parent segment", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,type,status,created_at")
		for _, activation := range resp.Activations {
			fmt.Printf("%s,%s,%s,%s,%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.Status, activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Activations) == 0 {
			fmt.Println("No matched activations found for parent segment")
			return
		}
		fmt.Printf("Matched Activations for Parent Segment %s:\n", args[0])
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS\tCREATED")
		for _, activation := range resp.Activations {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				activation.ID, activation.Name, activation.Type,
				activation.Status, activation.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d matched activations\n", len(resp.Activations))
	}
}

// HandleGetWorkflowProjectsForParentSegment gets workflow projects for a parent segment
func HandleGetWorkflowProjectsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Parent segment ID required")
	}

	resp, err := client.CDP.GetParentSegmentUserDefinedWorkflowProjects(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get workflow projects for parent segment", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,status,created_at")
		for _, project := range resp.Projects {
			fmt.Printf("%s,%s,%s,%s\n",
				project.ID, project.Name, project.Status,
				project.CreatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Projects) == 0 {
			fmt.Println("No workflow projects found for parent segment")
			return
		}
		fmt.Printf("Workflow Projects for Parent Segment %s:\n", args[0])
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tSTATUS\tCREATED")
		for _, project := range resp.Projects {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				project.ID, project.Name, project.Status,
				project.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d workflow projects\n", len(resp.Projects))
	}
}

// HandleGetWorkflowsForParentSegment gets workflows for a parent segment
func HandleGetWorkflowsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp activations workflows <parent-segment-id> <workflow-project-name>")
	}

	resp, err := client.CDP.GetParentSegmentUserDefinedWorkflows(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get workflows for parent segment", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,project_id,created_at")
		for _, workflow := range resp.Workflows {
			fmt.Printf("%s,%s,%s,%s\n",
				workflow.ID, workflow.Name, workflow.ProjectID,
				workflow.CreatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Workflows) == 0 {
			fmt.Println("No workflows found for parent segment")
			return
		}
		fmt.Printf("Workflows for Parent Segment %s:\n", args[0])
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPROJECT_ID\tCREATED")
		for _, workflow := range resp.Workflows {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				workflow.ID, workflow.Name, workflow.ProjectID,
				workflow.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d workflows\n", len(resp.Workflows))
	}
}
