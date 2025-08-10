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

// HandleListFunnels lists CDP funnels
func HandleListFunnels(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Audience ID required")
	}

	funnels, err := client.CDP.ListFunnels(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to list funnels", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		funnels := data.([]*td.CDPFunnel)
		var csvBuilder strings.Builder
		for _, funnel := range funnels {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%s\n",
				funnel.ID, funnel.Name,
				funnel.CreatedAt.Format("2006-01-02 15:04:05"),
				funnel.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		funnels := data.([]*td.CDPFunnel)
		if len(funnels) == 0 {
			return "No funnels found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tCREATED\tUPDATED")
		for _, funnel := range funnels {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				funnel.ID, funnel.Name,
				funnel.CreatedAt.Format("2006-01-02 15:04:05"),
				funnel.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d funnels\n", len(funnels)))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(funnels, flags.Format, flags.Output, "id,name,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleCreateFunnel creates a new CDP funnel
func HandleCreateFunnel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp funnel create <audience-id> <name> <steps-json>")
	}

	var stages []td.CDPFunnelStage
	err := json.Unmarshal([]byte(args[2]), &stages)
	if err != nil {
		log.Fatalf("Invalid stages JSON: %v", err)
	}

	req := td.CDPFunnelCreateRequest{
		Name:   args[1],
		Stages: stages,
	}

	funnel, err := client.CDP.CreateFunnel(ctx, args[0], req)
	if err != nil {
		handleError(err, "Failed to create funnel", flags.Verbose)
	}

	fmt.Printf("Funnel created successfully\n")
	fmt.Printf("ID: %s\n", funnel.ID)
	fmt.Printf("Name: %s\n", funnel.Name)
}

// HandleGetFunnel retrieves a specific funnel
func HandleGetFunnel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp funnel get <audience-id> <funnel-id>")
	}

	funnel, err := client.CDP.GetFunnel(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get funnel", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(funnel)
	case "csv":
		fmt.Println("id,name,step_count,created_at,updated_at")
		fmt.Printf("%s,%s,%d,%s,%s\n",
			funnel.ID, funnel.Name, len(funnel.Stages),
			funnel.CreatedAt.Format("2006-01-02 15:04:05"),
			funnel.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", funnel.ID)
		fmt.Printf("Name: %s\n", funnel.Name)
		fmt.Printf("Created: %s\n", funnel.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", funnel.UpdatedAt.Format("2006-01-02 15:04:05"))
		if len(funnel.Stages) > 0 {
			fmt.Printf("\nStages (%d):\n", len(funnel.Stages))
			for i, stage := range funnel.Stages {
				stageJSON, _ := json.MarshalIndent(stage, "", "  ")
				fmt.Printf("  %d. %s\n", i+1, stageJSON)
			}
		}
	}
}

// HandleUpdateFunnel updates a funnel
func HandleUpdateFunnel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Usage: cdp funnel update <audience-id> <funnel-id> <name> <steps-json>")
	}

	var stages []td.CDPFunnelStage
	err := json.Unmarshal([]byte(args[3]), &stages)
	if err != nil {
		log.Fatalf("Invalid stages JSON: %v", err)
	}

	req := td.CDPFunnelCreateRequest{
		Name:   args[2],
		Stages: stages,
	}

	funnel, err := client.CDP.UpdateFunnel(ctx, args[0], args[1], req)
	if err != nil {
		handleError(err, "Failed to update funnel", flags.Verbose)
	}

	fmt.Printf("Funnel %s updated successfully\n", funnel.ID)
}

// HandleDeleteFunnel deletes a funnel
func HandleDeleteFunnel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp funnel delete <audience-id> <funnel-id>")
	}

	_, err := client.CDP.DeleteFunnel(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to delete funnel", flags.Verbose)
	}

	fmt.Printf("Funnel %s deleted successfully\n", args[1])
}

// HandleCloneFunnel clones a funnel
func HandleCloneFunnel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp funnel clone <audience-id> <funnel-id> <new-name>")
	}

	req := td.CDPFunnelCloneRequest{
		Name: args[2],
	}
	funnel, err := client.CDP.CloneFunnel(ctx, args[0], args[1], req)
	if err != nil {
		handleError(err, "Failed to clone funnel", flags.Verbose)
	}

	fmt.Printf("Funnel cloned successfully\n")
	fmt.Printf("New Funnel ID: %s\n", funnel.ID)
	fmt.Printf("Name: %s\n", funnel.Name)
}

// HandleGetFunnelStatistics gets funnel statistics
func HandleGetFunnelStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp funnel statistics <audience-id> <funnel-id>")
	}

	stats, err := client.CDP.GetFunnelStatistics(ctx, args[0], args[1], nil)
	if err != nil {
		handleError(err, "Failed to get funnel statistics", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(stats)
	case "csv":
		fmt.Println("stage_id,history_count")
		for _, stage := range stats.Stages {
			fmt.Printf("%d,%d\n", stage.ID, len(stage.History))
		}
	default:
		if stats.Population != nil {
			fmt.Printf("Population: %d\n", *stats.Population)
		}
		if len(stats.Stages) > 0 {
			fmt.Printf("\nStage Statistics:\n")
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "STAGE ID\tHISTORY POINTS")
			for _, stage := range stats.Stages {
				fmt.Fprintf(w, "%d\t%d\n", stage.ID, len(stage.History))
			}
			w.Flush()
		}
	}
}

// HandleCreateEntityFunnel creates a new entity funnel
func HandleCreateEntityFunnel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp funnel create-entity <name> [description]")
	}

	desc := ""
	if len(args) > 1 {
		desc = args[1]
	}

	req := td.CDPFunnelEntityCreateRequest{
		Type: "funnel",
		Attributes: td.CDPFunnelEntityAttributes{
			Name:        args[0],
			Description: &desc,
			Stages:      []td.CDPFunnelStageEntity{}, // Empty stages for now
		},
	}

	funnel, err := client.CDP.CreateEntityFunnel(ctx, req)
	if err != nil {
		handleError(err, "Failed to create entity funnel", flags.Verbose)
	}

	if dataMap, ok := funnel.Data.(map[string]interface{}); ok {
		fmt.Printf("Entity funnel created successfully\n")
		if id, exists := dataMap["id"]; exists {
			fmt.Printf("ID: %s\n", id)
		}
		if attrs, ok := dataMap["attributes"].(map[string]interface{}); ok {
			if name, exists := attrs["name"]; exists {
				fmt.Printf("Name: %s\n", name)
			}
		}
	}
}

// HandleGetEntityFunnel retrieves an entity funnel
func HandleGetEntityFunnel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Usage: cdp funnel get-entity <funnel-id>")
	}

	funnel, err := client.CDP.GetEntityFunnel(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get entity funnel", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(funnel)
	default:
		if dataMap, ok := funnel.Data.(map[string]interface{}); ok {
			if id, exists := dataMap["id"]; exists {
				fmt.Printf("ID: %s\n", id)
			}
			if funnelType, exists := dataMap["type"]; exists {
				fmt.Printf("Type: %s\n", funnelType)
			}
			if attrs, ok := dataMap["attributes"].(map[string]interface{}); ok {
				if name, exists := attrs["name"]; exists {
					fmt.Printf("Name: %s\n", name)
				}
				if desc, exists := attrs["description"]; exists && desc != nil {
					fmt.Printf("Description: %s\n", desc)
				}
				if createdAt, exists := attrs["created_at"]; exists {
					fmt.Printf("Created: %s\n", createdAt)
				}
				if updatedAt, exists := attrs["updated_at"]; exists {
					fmt.Printf("Updated: %s\n", updatedAt)
				}
			}
		}
	}
}

// HandleUpdateEntityFunnel updates an entity funnel
func HandleUpdateEntityFunnel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Usage: cdp funnel update-entity <funnel-id> [key=value...]")
	}

	updates := make(map[string]interface{})

	// Parse key=value pairs from remaining arguments
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		updates[parts[0]] = parts[1]
	}

	// Parse flags for updates
	if flags.Name != "" {
		updates["name"] = flags.Name
	}
	if flags.Description != "" {
		updates["description"] = flags.Description
	}

	funnel, err := client.CDP.UpdateEntityFunnel(ctx, args[0], updates)
	if err != nil {
		handleError(err, "Failed to update entity funnel", flags.Verbose)
	}

	if dataMap, ok := funnel.Data.(map[string]interface{}); ok {
		if id, exists := dataMap["id"]; exists {
			fmt.Printf("Entity funnel %s updated successfully\n", id)
		}
	}
}
