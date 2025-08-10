package cdp

import (
	"context"
	"fmt"
	"log"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleSegmentCreate creates a new CDP segment
func HandleSegmentCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleSegmentList lists CDP segments
func HandleSegmentList(ctx context.Context, client *td.Client, args []string, flags Flags) {
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
				segment.ID, segment.Name, segment.Population,
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
				segment.ID, segment.Name, segment.Population,
				segment.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d segments\n", resp.Total))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "id,name,population,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleSegmentGet retrieves a specific CDP segment
func HandleSegmentGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleSegmentUpdate updates a CDP segment
func HandleSegmentUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleSegmentDelete deletes a CDP segment
func HandleSegmentDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment delete <audience-id> <segment-id>")
	}

	err := client.CDP.DeleteSegment(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to delete segment", flags.Verbose)
	}

	fmt.Printf("Segment %s deleted successfully\n", args[0])
}

// HandleSegmentFolders lists segments in a folder
func HandleSegmentFolders(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment folders <audience-id> <folder-id>")
	}

	opts := &td.CDPSegmentListOptions{
		Limit:  100,
		Offset: 0,
	}

	response, err := client.CDP.ListSegmentsInFolder(ctx, args[0], args[1], opts)
	if err != nil {
		handleError(err, "Failed to get segments in folder", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		segments := data.([]*td.CDPSegment)
		var csvBuilder strings.Builder
		for _, segment := range segments {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d,%s,%s\n",
				segment.ID, segment.Name, segment.Population,
				segment.CreatedAt.Format("2006-01-02 15:04:05"),
				segment.UpdatedAt.Format("2006-01-02 15:04:05")))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		segments := data.([]*td.CDPSegment)
		if len(segments) == 0 {
			return "No segments found in folder\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPROFILES\tCREATED")
		for _, segment := range segments {
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\n",
				segment.ID, segment.Name, segment.Population,
				segment.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d segments\n", len(segments)))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(response.Segments, flags.Format, flags.Output, "id,name,population,created_at,updated_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleSegmentQuery executes a query for a segment
func HandleSegmentQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment query <audience-id> <query>")
	}

	resp, err := client.CDP.CreateSegmentQuery(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to query segment", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPSegmentQuery)
		return fmt.Sprintf("%s,%s,%s,%s\n", resp.ID, resp.Status, resp.Error, resp.CreatedAt.Format("2006-01-02 15:04:05"))
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPSegmentQuery)
		var tableBuilder strings.Builder
		fmt.Fprintf(&tableBuilder, "Query ID: %s\n", resp.ID)
		fmt.Fprintf(&tableBuilder, "Status: %s\n", resp.Status)
		if resp.Error != "" {
			fmt.Fprintf(&tableBuilder, "Error: %s\n", resp.Error)
		}
		fmt.Fprintf(&tableBuilder, "Created: %s\n", resp.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(&tableBuilder, "Updated: %s\n", resp.UpdatedAt.Format("2006-01-02 15:04:05"))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "query_id,status,error,created_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleSegmentNewQuery creates a new segment query
func HandleSegmentNewQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment new-query <audience-id> <query>")
	}

	segmentQuery, err := client.CDP.CreateSegmentQuery(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to create segment query", flags.Verbose)
	}

	fmt.Printf("Query started successfully\n")
	fmt.Printf("Query ID: %s\n", segmentQuery.ID)
}

// HandleSegmentQueryStatus gets the status of a segment query
func HandleSegmentQueryStatus(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment query-status <audience-id> <query-id>")
	}

	status, err := client.CDP.GetSegmentQueryStatus(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get query status", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		status := data.(*td.CDPSegmentQuery)
		return fmt.Sprintf("%s,%s,%s,%s\n", status.ID, status.Status, status.Error, status.CreatedAt.Format("2006-01-02 15:04:05"))
	}

	tableFormatter := func(data interface{}) string {
		status := data.(*td.CDPSegmentQuery)
		var tableBuilder strings.Builder
		fmt.Fprintf(&tableBuilder, "Query ID: %s\n", status.ID)
		fmt.Fprintf(&tableBuilder, "Status: %s\n", status.Status)
		if status.Error != "" {
			fmt.Fprintf(&tableBuilder, "Error: %s\n", status.Error)
		}
		fmt.Fprintf(&tableBuilder, "Created: %s\n", status.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(&tableBuilder, "Updated: %s\n", status.UpdatedAt.Format("2006-01-02 15:04:05"))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(status, flags.Format, flags.Output, "query_id,status,error,created_at", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleSegmentKillQuery kills a running segment query
func HandleSegmentKillQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment kill-query <audience-id> <query-id>")
	}

	err := client.CDP.KillSegmentQuery(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to kill segment query", flags.Verbose)
	}

	fmt.Printf("Query %s killed successfully\n", args[1])
}

// HandleSegmentCustomers gets customers in a segment
func HandleSegmentCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment customers <audience-id> <segment-id>")
	}

	opts := &td.CDPSegmentCustomerListOptions{
		Limit: 100,
	}

	resp, err := client.CDP.GetSegmentQueryCustomers(ctx, args[0], args[1], opts)
	if err != nil {
		handleError(err, "Failed to get segment customers", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPSegmentCustomerListResponse)
		var csvBuilder strings.Builder
		for _, customer := range resp.Customers {
			csvBuilder.WriteString(fmt.Sprintf("%s\n", customer.ID))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPSegmentCustomerListResponse)
		if len(resp.Customers) == 0 {
			return "No customers found\n"
		}
		var tableBuilder strings.Builder
		fmt.Fprintf(&tableBuilder, "Customer IDs:\n")
		for _, customer := range resp.Customers {
			fmt.Fprintf(&tableBuilder, "  %s\n", customer.ID)
		}
		fmt.Fprintf(&tableBuilder, "\nTotal: %d customers\n", resp.Total)
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(resp, flags.Format, flags.Output, "customer_id", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleSegmentStatistics gets statistics for a segment
func HandleSegmentStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp segment statistics <audience-id> <segment-id>")
	}

	stats, err := client.CDP.GetSegmentStatistics(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get segment statistics", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(stats)
	case "csv":
		fmt.Println("timestamp,count,has_data")
		for _, point := range stats {
			if len(point) >= 3 {
				fmt.Printf("%v,%v,%v\n", point[0], point[1], point[2])
			}
		}
	default:
		fmt.Println("Segment Statistics:")
		if len(stats) == 0 {
			fmt.Println("No statistics available")
		} else {
			for _, point := range stats {
				if len(point) >= 3 {
					fmt.Printf("  Timestamp: %v, Count: %v, Has Data: %v\n", point[0], point[1], point[2])
				}
			}
		}
	}
}

// HandleCreateEntitySegment creates a new entity segment
func HandleCreateEntitySegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Usage: cdp segment create-entity <name> <description> <segment-type> <parent-folder-id>")
	}

	attributes := make(map[string]interface{})
	if flags.Query != "" {
		attributes["query"] = flags.Query
	}

	segment, err := client.CDP.CreateEntitySegment(ctx, args[0], args[1], args[2], args[3], attributes)
	if err != nil {
		handleError(err, "Failed to create entity segment", flags.Verbose)
	}

	fmt.Printf("Entity segment created successfully\n")
	if data, ok := segment.Data.(map[string]interface{}); ok {
		if id, ok := data["id"]; ok {
			fmt.Printf("ID: %v\n", id)
		}
		if attrs, ok := data["attributes"].(map[string]interface{}); ok {
			if name, ok := attrs["name"]; ok {
				fmt.Printf("Name: %v\n", name)
			}
		}
	}
}

// HandleGetEntitySegment retrieves an entity segment
func HandleGetEntitySegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Usage: cdp segment get-entity <segment-id>")
	}

	segment, err := client.CDP.GetEntitySegment(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to get entity segment", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(segment)
	default:
		if data, ok := segment.Data.(map[string]interface{}); ok {
			if id, ok := data["id"]; ok {
				fmt.Printf("ID: %v\n", id)
			}
			if segType, ok := data["type"]; ok {
				fmt.Printf("Type: %v\n", segType)
			}
			if attrs, ok := data["attributes"].(map[string]interface{}); ok {
				if name, ok := attrs["name"]; ok {
					fmt.Printf("Name: %v\n", name)
				}
				if desc, ok := attrs["description"]; ok {
					fmt.Printf("Description: %v\n", desc)
				}
				if count, ok := attrs["profile_count"]; ok {
					fmt.Printf("Profile Count: %v\n", count)
				}
				if query, ok := attrs["query"]; ok {
					fmt.Printf("Query: %v\n", query)
				}
			}
		}
	}
}

// HandleListEntitySegments lists all entity segments
func HandleListEntitySegments(ctx context.Context, client *td.Client, flags Flags) {
	segments, err := client.CDP.ListEntitySegments(ctx)
	if err != nil {
		handleError(err, "Failed to list entity segments", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		resp := data.(*td.CDPJSONAPIListResponse)
		var csvBuilder strings.Builder
		for _, segment := range resp.Data {
			name := ""
			count := ""
			if n, ok := segment.Attributes["name"]; ok {
				name = fmt.Sprintf("%v", n)
			}
			if c, ok := segment.Attributes["profile_count"]; ok {
				count = fmt.Sprintf("%v", c)
			}
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s\n", segment.ID, name, count))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		resp := data.(*td.CDPJSONAPIListResponse)
		if len(resp.Data) == 0 {
			return "No entity segments found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPROFILES")
		for _, segment := range resp.Data {
			name := ""
			count := ""
			if n, ok := segment.Attributes["name"]; ok {
				name = fmt.Sprintf("%v", n)
			}
			if c, ok := segment.Attributes["profile_count"]; ok {
				count = fmt.Sprintf("%v", c)
			}
			fmt.Fprintf(w, "%s\t%s\t%s\n", segment.ID, name, count)
		}
		w.Flush()
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(segments, flags.Format, flags.Output, "id,name,profile_count", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

// HandleUpdateEntitySegment updates an entity segment
func HandleUpdateEntitySegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Usage: cdp segment update-entity <segment-id> [options]")
	}

	updates := make(map[string]interface{})

	// Parse flags for updates
	if flags.Name != "" {
		updates["name"] = flags.Name
	}
	if flags.Description != "" {
		updates["description"] = flags.Description
	}
	if flags.Query != "" {
		updates["query"] = flags.Query
	}

	segment, err := client.CDP.UpdateEntitySegment(ctx, args[0], updates)
	if err != nil {
		handleError(err, "Failed to update entity segment", flags.Verbose)
	}

	fmt.Printf("Entity segment updated successfully\n")
	if data, ok := segment.Data.(map[string]interface{}); ok {
		if id, ok := data["id"]; ok {
			fmt.Printf("ID: %v\n", id)
		}
	}
}

// HandleDeleteEntitySegment deletes an entity segment
func HandleDeleteEntitySegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Usage: cdp segment delete-entity <segment-id>")
	}

	err := client.CDP.DeleteEntitySegment(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to delete entity segment", flags.Verbose)
	}

	fmt.Printf("Entity segment %s deleted successfully\n", args[0])
}
