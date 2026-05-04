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
func HandleListTokens(ctx context.Context, client *td.Client, cmd interface{}, flags Flags) error {
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
		return wrapError(err, "failed to list tokens", flags.Verbose)
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
		return wrapError(err, "failed to write output", flags.Verbose)
	}
	return nil
}

// HandleGetEntityToken gets an entity token
func HandleGetEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Token ID required")
	}

	token, err := client.CDP.GetEntityToken(ctx, args[0])
	if err != nil {
		return wrapError(err, "failed to get entity token", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		return printJSON(token)
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
	return nil
}

// HandleUpdateEntityToken updates an entity token
func HandleUpdateEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Token ID and updates (key=value) required")
	}

	tokenID := args[0]
	req := &td.CDPTokenUpdateRequest{}

	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			return usageError(fmt.Sprintf("Invalid update format: %s (expected key=value)", arg))
		}
		switch parts[0] {
		case "name":
			req.Name = parts[1]
		case "description":
			req.Description = parts[1]
		case "status":
			req.Status = parts[1]
		case "scopes":
			req.Scopes = strings.Split(parts[1], ",")
		case "metadata":
			var metadata map[string]interface{}
			if err := json.Unmarshal([]byte(parts[1]), &metadata); err != nil {
				return usageError(fmt.Sprintf("Invalid metadata JSON: %v", err))
			}
			req.Metadata = metadata
		default:
			return usageError(fmt.Sprintf("Unknown field: %s", parts[0]))
		}
	}

	token, err := client.CDP.UpdateEntityToken(ctx, tokenID, req)
	if err != nil {
		return wrapError(err, "failed to update entity token", flags.Verbose)
	}

	fmt.Printf("Entity token %s updated successfully\n", token.ID)
	return nil
}

// HandleDeleteEntityToken deletes an entity token
func HandleDeleteEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Token ID required")
	}

	if err := client.CDP.DeleteEntityToken(ctx, args[0]); err != nil {
		return wrapError(err, "failed to delete entity token", flags.Verbose)
	}

	fmt.Printf("Entity token %s deleted successfully\n", args[0])
	return nil
}

// HandleCreateToken creates a legacy token (audience-level)
func HandleCreateToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 4 {
		return usageError("Usage: cdp token create <audience-id> <key-column> <attribute-columns-json> [description]")
	}

	var attributeColumns []string
	if err := json.Unmarshal([]byte(args[2]), &attributeColumns); err != nil {
		return usageError(fmt.Sprintf("Invalid attribute columns JSON: %v", err))
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
		return wrapError(err, "failed to create token", flags.Verbose)
	}

	fmt.Printf("Token created successfully\n")
	fmt.Printf("ID: %s\n", token.ID)
	fmt.Printf("Name: %s\n", token.Name)
	return nil
}

// HandleGetToken gets a legacy token
func HandleGetToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Usage: cdp token get <audience-id> <token-id>")
	}

	token, err := client.CDP.GetToken(ctx, args[0], args[1])
	if err != nil {
		return wrapError(err, "failed to get token", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		return printJSON(token)
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
	return nil
}

// HandleUpdateToken updates a legacy token
func HandleUpdateToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return usageError("Usage: cdp token update <audience-id> <token-id> <key-column> <attribute-columns-json> [description]")
	}

	var attributeColumns []string
	if err := json.Unmarshal([]byte(args[3]), &attributeColumns); err != nil {
		return usageError(fmt.Sprintf("Invalid attribute columns JSON: %v", err))
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
		return wrapError(err, "failed to update token", flags.Verbose)
	}

	fmt.Printf("Token %s updated successfully\n", token.ID)
	return nil
}

// HandleDeleteToken deletes a legacy token
func HandleDeleteToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Usage: cdp token delete <audience-id> <token-id>")
	}

	if err := client.CDP.DeleteToken(ctx, args[0], args[1]); err != nil {
		return wrapError(err, "failed to delete token", flags.Verbose)
	}

	fmt.Printf("Token %s deleted successfully\n", args[1])
	return nil
}

// HandleCreateEntityToken creates an entity token
func HandleCreateEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("JSON string with token details required")
	}

	var req td.CDPTokenCreateRequest
	if err := json.Unmarshal([]byte(args[0]), &req); err != nil {
		return usageError(fmt.Sprintf("Invalid JSON: %v", err))
	}

	token, err := client.CDP.CreateEntityToken(ctx, &req)
	if err != nil {
		return wrapError(err, "failed to create entity token", flags.Verbose)
	}

	fmt.Printf("Entity token created successfully\n")
	fmt.Printf("ID: %s\n", token.ID)
	fmt.Printf("Name: %s\n", token.Name)
	return nil
}
