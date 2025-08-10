package cdp

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleAudienceCreate creates a new CDP audience
func HandleAudienceCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleAudienceList lists all CDP audiences
func HandleAudienceList(ctx context.Context, client *td.Client, flags Flags) {
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

// HandleAudienceGet retrieves a specific CDP audience
func HandleAudienceGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleAudienceDelete deletes a CDP audience
func HandleAudienceDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	err := client.CDP.DeleteAudience(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to delete audience", flags.Verbose)
	}

	fmt.Printf("Audience %s deleted successfully\n", args[0])
}

// HandleAudienceUpdate updates a CDP audience
func HandleAudienceUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp audience update <audience-id> <key=value>...")
	}

	audienceID := args[0]
	req := &td.CDPAudienceUpdateRequest{}

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
		case "schedule_type":
			req.ScheduleType = parts[1]
		case "schedule_option":
			req.ScheduleOption = &parts[1]
		case "timezone":
			req.Timezone = parts[1]
		case "workflow_hive_only":
			if parts[1] == "true" {
				req.WorkflowHiveOnly = &[]bool{true}[0]
			} else {
				req.WorkflowHiveOnly = &[]bool{false}[0]
			}
		case "hive_engine_version":
			req.HiveEngineVersion = parts[1]
		case "hive_pool_name":
			req.HivePoolName = &parts[1]
		case "presto_pool_name":
			req.PrestoPoolName = &parts[1]
		default:
			log.Fatalf("Unknown field: %s", parts[0])
		}
	}

	audience, err := client.CDP.UpdateAudience(ctx, audienceID, req)
	if err != nil {
		handleError(err, "Failed to update audience", flags.Verbose)
	}

	fmt.Printf("Audience %s updated successfully\n", audience.ID)
}

// HandleAudienceAttributes gets audience attributes
func HandleAudienceAttributes(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	attributes, err := client.CDP.GetAudienceAttributes(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get audience attributes", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(attributes)
	case "csv":
		fmt.Println("name,type,parent_database_name,parent_table_name,parent_column")
		for _, attr := range attributes {
			if attrMap, ok := attr.(map[string]interface{}); ok {
				fmt.Printf("%v,%v,%v,%v,%v\n",
					attrMap["name"], attrMap["type"], attrMap["parentDatabaseName"],
					attrMap["parentTableName"], attrMap["parentColumn"])
			}
		}
	default:
		if len(attributes) == 0 {
			fmt.Println("No attributes found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tTYPE\tDATABASE\tTABLE\tCOLUMN")
		for _, attr := range attributes {
			if attrMap, ok := attr.(map[string]interface{}); ok {
				fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\n",
					attrMap["name"], attrMap["type"], attrMap["parentDatabaseName"],
					attrMap["parentTableName"], attrMap["parentColumn"])
			}
		}
		w.Flush()
		fmt.Printf("\nTotal: %d attributes\n", len(attributes))
	}
}

// HandleAudienceBehaviors gets audience behaviors
func HandleAudienceBehaviors(ctx context.Context, client *td.Client, args []string, flags Flags) {
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
		fmt.Println("id,name")
		for _, behavior := range behaviors {
			fmt.Printf("%s,%s\n", behavior.ID, behavior.Name)
		}
	default:
		if len(behaviors) == 0 {
			fmt.Println("No behaviors found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME")
		for _, behavior := range behaviors {
			fmt.Fprintf(w, "%s\t%s\n", behavior.ID, behavior.Name)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d behaviors\n", len(behaviors))
	}
}

// HandleAudienceRun runs an audience execution
func HandleAudienceRun(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	execution, err := client.CDP.RunAudience(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to run audience", flags.Verbose)
	}

	fmt.Printf("Audience execution started successfully\n")
	fmt.Printf("Audience ID: %s\n", execution.AudienceID)
	fmt.Printf("Status: %s\n", execution.Status)
	fmt.Printf("Created: %s\n", execution.CreatedAt.Format("2006-01-02 15:04:05"))
}

// HandleAudienceExecutions gets audience execution history
func HandleAudienceExecutions(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	executions, err := client.CDP.GetAudienceExecutions(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get audience executions", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		executions := data.([]*td.CDPAudienceExecution)
		var csvBuilder strings.Builder
		for _, exec := range executions {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s\n",
				exec.AudienceID, exec.Status, exec.CreatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		executions := data.([]*td.CDPAudienceExecution)
		if len(executions) == 0 {
			return "No executions found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "AUDIENCE_ID\tSTATUS\tCREATED")
		for _, exec := range executions {
			fmt.Fprintf(w, "%s\t%s\t%s\n",
				exec.AudienceID, exec.Status, exec.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d executions\n", len(executions)))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(executions, flags.Format, flags.Output, "audience_id,status,created_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleAudienceStatistics gets audience statistics
func HandleAudienceStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	stats, err := client.CDP.GetAudienceStatistics(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get audience statistics", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(stats)
	case "csv":
		fmt.Println("data_point")
		for _, point := range stats {
			fmt.Printf("%v\n", point)
		}
	default:
		if len(stats) == 0 {
			fmt.Println("No statistics available")
			return
		}

		fmt.Printf("Statistics data points:\n")
		for i, point := range stats {
			fmt.Printf("  %d. %v\n", i+1, point)
		}
		fmt.Printf("\nTotal data points: %d\n", len(stats))
	}
}

// HandleAudienceSampleValues gets audience sample values
func HandleAudienceSampleValues(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Audience ID and attribute name required")
	}

	values, err := client.CDP.GetAudienceSampleValues(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get sample values", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(values)
	case "csv":
		fmt.Println("value")
		for _, value := range values {
			fmt.Printf("%v\n", value)
		}
	default:
		if len(values) == 0 {
			fmt.Println("No sample values found")
			return
		}

		fmt.Printf("Sample values for attribute '%s':\n", args[1])
		for i, value := range values {
			fmt.Printf("  %d. %v\n", i+1, value)
		}
		fmt.Printf("\nTotal: %d values\n", len(values))
	}
}

// HandleAudienceBehaviorSamples gets behavior sample values
func HandleAudienceBehaviorSamples(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp audience behavior-samples <audience-id> <behavior-id> <column>")
	}

	samples, err := client.CDP.GetAudienceBehaviorSampleValues(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to get audience behavior sample values", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(samples)
	case "csv":
		fmt.Println("value,frequency")
		for _, sample := range samples {
			if len(sample) >= 2 {
				fmt.Printf("%v,%v\n", sample[0], sample[1])
			}
		}
	default:
		if len(samples) == 0 {
			fmt.Println("No sample values found")
			return
		}
		fmt.Printf("Sample Values for Behavior %s, Column %s:\n", args[1], args[2])
		for _, sample := range samples {
			if len(sample) >= 2 {
				fmt.Printf("  Value: %v, Frequency: %v\n", sample[0], sample[1])
			}
		}
		fmt.Printf("\nTotal: %d samples\n", len(samples))
	}
}
