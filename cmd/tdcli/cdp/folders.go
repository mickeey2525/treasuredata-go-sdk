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

// HandleCreateAudienceFolder creates a new audience folder
func HandleCreateAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleUpdateAudienceFolder updates an audience folder
func HandleUpdateAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Usage: cdp folder update <audience-id> <folder-id> <key=value>...")
	}

	audienceID := args[0]
	folderID := args[1]
	req := &td.CDPAudienceFolderUpdateRequest{}

	// Parse key=value pairs
	for _, arg := range args[2:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		switch parts[0] {
		case "name":
			req.Name = parts[1]
		case "description":
			req.Description = parts[1]
		default:
			log.Fatalf("Unknown field: %s", parts[0])
		}
	}

	folder, err := client.CDP.UpdateAudienceFolder(ctx, audienceID, folderID, req)
	if err != nil {
		handleError(err, "Failed to update audience folder", flags.Verbose)
	}

	fmt.Printf("Audience folder %s updated successfully\n", folder.ID)
}

// HandleDeleteAudienceFolder deletes an audience folder
func HandleDeleteAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp folder delete <audience-id> <folder-id>")
	}

	err := client.CDP.DeleteAudienceFolder(ctx, args[0], args[1])
	if err != nil {
		handleError(err, "Failed to delete audience folder", flags.Verbose)
	}

	fmt.Printf("Audience folder %s deleted successfully\n", args[1])
}

// HandleGetAudienceFolder gets an audience folder
func HandleGetAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleListFolders lists all folders in an audience
func HandleListFolders(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleCreateEntityFolder creates an entity folder
func HandleCreateEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
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

// HandleGetEntityFolder gets an entity folder
func HandleGetEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
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
	default:
		if dataMap, ok := folder.Data.(map[string]interface{}); ok {
			if id, exists := dataMap["id"]; exists {
				fmt.Printf("ID: %s\n", id)
			}
			if folderType, exists := dataMap["type"]; exists {
				fmt.Printf("Type: %s\n", folderType)
			}
			if attrs, ok := dataMap["attributes"].(map[string]interface{}); ok {
				if name, exists := attrs["name"]; exists {
					fmt.Printf("Name: %s\n", name)
				}
				if desc, exists := attrs["description"]; exists && desc != "" {
					fmt.Printf("Description: %s\n", desc)
				}
				if parentID, exists := attrs["parent_folder_id"]; exists && parentID != nil {
					fmt.Printf("Parent Folder ID: %s\n", parentID)
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

// HandleUpdateEntityFolder updates an entity folder
func HandleUpdateEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Usage: cdp folder update-entity <folder-id> <key=value>...")
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

// HandleDeleteEntityFolder deletes an entity folder
func HandleDeleteEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Folder ID required")
	}

	err := client.CDP.DeleteEntityFolder(ctx, args[0])
	if err != nil {
		handleError(err, "Failed to delete entity folder", flags.Verbose)
	}

	fmt.Printf("Entity folder %s deleted successfully\n", args[0])
}

// HandleGetEntitiesByFolder gets entities in a folder
func HandleGetEntitiesByFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Folder ID required")
	}

	folderID := args[0]

	entities, err := client.CDP.GetEntitiesByFolder(ctx, folderID)
	if err != nil {
		handleError(err, "Failed to get entities by folder", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(entities)
	case "csv":
		fmt.Println("id,type,name")
		for _, entity := range entities.Data {
			name := ""
			if entity.Attributes != nil {
				if nameVal, ok := entity.Attributes["name"]; ok {
					name = fmt.Sprintf("%v", nameVal)
				}
			}
			fmt.Printf("%s,%s,%s\n", entity.ID, entity.Type, name)
		}
	default:
		if len(entities.Data) == 0 {
			fmt.Println("No entities found in folder")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tTYPE\tNAME")
		for _, entity := range entities.Data {
			name := ""
			if entity.Attributes != nil {
				if nameVal, ok := entity.Attributes["name"]; ok {
					name = fmt.Sprintf("%v", nameVal)
				}
			}
			fmt.Fprintf(w, "%s\t%s\t%s\n", entity.ID, entity.Type, name)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d entities\n", len(entities.Data))
	}
}
