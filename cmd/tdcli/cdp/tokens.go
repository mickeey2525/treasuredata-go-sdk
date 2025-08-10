package cdp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// CDPTokensListCmd represents the list tokens command
type CDPTokensListCmd struct {
	AudienceID string `arg:"" help:"Audience ID"`
	Limit      int    `help:"Limit number of results" default:"100"`
	Offset     int    `help:"Offset for pagination" default:"0"`
	Type       string `help:"Filter by token type"`
	Status     string `help:"Filter by token status"`
}

// HandleListTokens lists CDP tokens
func HandleListTokens(ctx context.Context, client *td.Client, cmd interface{}, flags Flags) {
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

// HandleGetEntityToken gets an entity token
func HandleGetEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Token ID required", flags.Verbose)
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

// HandleUpdateEntityToken updates an entity token
func HandleUpdateEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		handleUsageError("Token ID and updates (key=value) required", flags.Verbose)
	}

	tokenID := args[0]
	req := &td.CDPTokenUpdateRequest{}

	// Parse key=value pairs
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			handleUsageError(fmt.Sprintf("Invalid update format: %s (expected key=value)", arg), flags.Verbose)
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
				handleUsageError(fmt.Sprintf("Invalid metadata JSON: %v", err), flags.Verbose)
			}
			req.Metadata = metadata
		default:
			handleUsageError(fmt.Sprintf("Unknown field: %s", parts[0]), flags.Verbose)
		}
	}

	token, err := client.CDP.UpdateEntityToken(ctx, tokenID, req)
	if err != nil {
		handleError(err, "Failed to update entity token", flags.Verbose)
	}

	fmt.Printf("Entity token %s updated successfully\n", token.ID)
}

// HandleDeleteEntityToken deletes an entity token
func HandleDeleteEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("Token ID required", flags.Verbose)
	}

	err := client.CDP.DeleteEntityToken(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to delete entity token", flags.Verbose)
	}

	fmt.Printf("Entity token %s deleted successfully\n", args[0])
}

// HandleCreateToken creates a legacy token (audience-level)
func HandleCreateToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		handleUsageError("Usage: cdp token create <audience-id> <key-column> <attribute-columns-json> [description]", flags.Verbose)
	}

	var attributeColumns []string
	err := json.Unmarshal([]byte(args[2]), &attributeColumns)
	if err != nil {
		handleUsageError(fmt.Sprintf("Invalid attribute columns JSON: %v", err), flags.Verbose)
	}

	req := &td.CDPLegacyTokenRequest{
		KeyColumn:        args[1],
		AttributeColumns: attributeColumns,
	}
	if len(args) > 3 && args[3] != "" {
		req.Description = args[3]
	}

	token, err := client.CDP.CreateToken(ctx, args[0], req)
	if err != nil {
		handleError(err, "Failed to create token", flags.Verbose)
	}

	fmt.Printf("Token created successfully\n")
	fmt.Printf("ID: %s\n", token.ID)
	fmt.Printf("Name: %s\n", token.Name)
}

// HandleGetToken gets a legacy token
func HandleGetToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		handleUsageError("Usage: cdp token get <audience-id> <token-id>", flags.Verbose)
	}

	token, err := client.CDP.GetToken(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to get token", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(token)
	case "csv":
		fmt.Println("id,name,type,status,description,created_at,updated_at")
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s\n",
			token.ID, token.Name, token.Type, token.Status, token.Description,
			token.CreatedAt.Format("2006-01-02 15:04:05"),
			token.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", token.ID)
		fmt.Printf("Name: %s\n", token.Name)
		fmt.Printf("Type: %s\n", token.Type)
		fmt.Printf("Status: %s\n", token.Status)
		fmt.Printf("Description: %s\n", token.Description)
		if len(token.Scopes) > 0 {
			fmt.Printf("Scopes: %s\n", strings.Join(token.Scopes, ", "))
		}
		fmt.Printf("Created: %s\n", token.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", token.UpdatedAt.Format("2006-01-02 15:04:05"))
	}
}

// HandleUpdateToken updates a legacy token
func HandleUpdateToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		handleUsageError("Usage: cdp token update <audience-id> <token-id> <key-column> <attribute-columns-json> [description]", flags.Verbose)
	}

	var attributeColumns []string
	err := json.Unmarshal([]byte(args[3]), &attributeColumns)
	if err != nil {
		handleUsageError(fmt.Sprintf("Invalid attribute columns JSON: %v", err), flags.Verbose)
	}

	req := &td.CDPLegacyTokenRequest{
		KeyColumn:        args[2],
		AttributeColumns: attributeColumns,
	}
	if len(args) > 4 && args[4] != "" {
		req.Description = args[4]
	}

	token, err := client.CDP.UpdateToken(ctx, args[0], args[1], req)
	if err != nil {
		handleError(err, "Failed to update token", flags.Verbose)
	}

	fmt.Printf("Token %s updated successfully\n", token.ID)
}

// HandleDeleteToken deletes a legacy token
func HandleDeleteToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		handleUsageError("Usage: cdp token delete <audience-id> <token-id>", flags.Verbose)
	}

	err := client.CDP.DeleteToken(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to delete token", flags.Verbose)
	}

	fmt.Printf("Token %s deleted successfully\n", args[1])
}

// HandleCreateEntityToken creates an entity token
func HandleCreateEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		handleUsageError("JSON string with token details required", flags.Verbose)
	}

	var req td.CDPTokenCreateRequest
	err := json.Unmarshal([]byte(args[0]), &req)
	if err != nil {
		handleUsageError(fmt.Sprintf("Invalid JSON: %v", err), flags.Verbose)
	}

	token, err := client.CDP.CreateEntityToken(ctx, &req)
	if err != nil {
		handleError(err, "Failed to create entity token", flags.Verbose)
	}

	fmt.Printf("Entity token created successfully\n")
	fmt.Printf("ID: %s\n", token.ID)
	fmt.Printf("Name: %s\n", token.Name)
}
