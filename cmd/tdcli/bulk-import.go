package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleBulkImportCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printBulkImportUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "list", "ls":
		handleBulkImportList(ctx, client, flags)
	case "get", "show":
		handleBulkImportGet(ctx, client, subArgs, flags)
	case "create":
		handleBulkImportCreate(ctx, client, subArgs, flags)
	case "delete", "rm":
		handleBulkImportDelete(ctx, client, subArgs, flags)
	case "upload":
		handleBulkImportUpload(ctx, client, subArgs, flags)
	case "commit":
		handleBulkImportCommit(ctx, client, subArgs, flags)
	case "perform":
		handleBulkImportPerform(ctx, client, subArgs, flags)
	case "freeze":
		handleBulkImportFreeze(ctx, client, subArgs, flags)
	case "unfreeze":
		handleBulkImportUnfreeze(ctx, client, subArgs, flags)
	case "parts":
		handleBulkImportParts(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown bulk import subcommand: %s\n", subcommand)
		printBulkImportUsage()
		os.Exit(1)
	}
}

func printBulkImportUsage() {
	fmt.Printf(`Bulk Import Management Commands

USAGE:
    tdcli bulk-import <subcommand> [options]
    tdcli import <subcommand> [options]

SUBCOMMANDS:
    list, ls               List bulk import sessions
    get, show <session>    Get bulk import session details
    create <session> <database> <table>  Create a new bulk import session
    delete, rm <session>   Delete a bulk import session
    upload <session> <part> <file>  Upload a part to session
    commit <session>       Commit a bulk import session
    perform <session>      Perform bulk import job
    freeze <session>       Freeze a bulk import session
    unfreeze <session>     Unfreeze a bulk import session
    parts <session>        List parts in a bulk import session

OPTIONS:
    --format FORMAT        Output format (json, table, csv)
    --verbose, -v          Verbose output

EXAMPLES:
    tdcli import list
    tdcli import show my_session
    tdcli import create my_session my_db my_table
    tdcli import upload my_session part1 data.csv
    tdcli import commit my_session
    tdcli import perform my_session
    tdcli import parts my_session

`)
}

func handleBulkImportList(ctx context.Context, client *td.Client, flags Flags) {
	bulkImports, err := client.BulkImport.List(ctx)
	handleError(err, "Failed to list bulk import sessions", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(bulkImports)
	case "csv":
		printBulkImportsCSV(bulkImports)
	default:
		printBulkImportsTable(bulkImports)
	}
}

func handleBulkImportGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Session name required")
		fmt.Println("Usage: tdcli import get <session_name>")
		os.Exit(1)
	}

	sessionName := args[0]
	bulkImport, err := client.BulkImport.Show(ctx, sessionName)
	handleError(err, "Failed to get bulk import session", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(bulkImport)
	case "csv":
		printBulkImportsCSV([]td.BulkImport{*bulkImport})
	default:
		printBulkImportDetails(*bulkImport)
	}
}

func handleBulkImportCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		fmt.Println("Error: Session name, database, and table required")
		fmt.Println("Usage: tdcli import create <session_name> <database> <table>")
		os.Exit(1)
	}

	sessionName := args[0]
	database := args[1]
	table := args[2]

	err := client.BulkImport.Create(ctx, sessionName, database, table)
	handleError(err, "Failed to create bulk import session", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully created bulk import session: %s for %s.%s\n", sessionName, database, table)
	} else {
		fmt.Printf("Created bulk import session: %s\n", sessionName)
	}
}

func handleBulkImportDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Session name required")
		fmt.Println("Usage: tdcli import delete <session_name>")
		os.Exit(1)
	}

	sessionName := args[0]

	// Confirm deletion
	fmt.Printf("Are you sure you want to delete bulk import session '%s'? (y/N): ", sessionName)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return
	}

	err := client.BulkImport.Delete(ctx, sessionName)
	handleError(err, "Failed to delete bulk import session", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully deleted bulk import session: %s\n", sessionName)
	} else {
		fmt.Printf("Deleted bulk import session: %s\n", sessionName)
	}
}

func handleBulkImportUpload(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		fmt.Println("Error: Session name, part name, and file path required")
		fmt.Println("Usage: tdcli import upload <session_name> <part_name> <file_path>")
		os.Exit(1)
	}

	sessionName := args[0]
	partName := args[1]
	filePath := args[2]

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		handleError(err, "Failed to open file", flags.Verbose)
		return
	}
	defer file.Close()

	if flags.Verbose {
		fmt.Printf("Uploading file %s as part %s to session %s...\n", filePath, partName, sessionName)
	}

	err = client.BulkImport.UploadPart(ctx, sessionName, partName, file)
	handleError(err, "Failed to upload part", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully uploaded part %s to session %s\n", partName, sessionName)
	} else {
		fmt.Printf("Uploaded part: %s\n", partName)
	}
}

func handleBulkImportCommit(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Session name required")
		fmt.Println("Usage: tdcli import commit <session_name>")
		os.Exit(1)
	}

	sessionName := args[0]

	// Confirm commit
	fmt.Printf("Are you sure you want to commit bulk import session '%s'? (y/N): ", sessionName)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Commit cancelled")
		return
	}

	err := client.BulkImport.Commit(ctx, sessionName)
	handleError(err, "Failed to commit bulk import session", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully committed bulk import session: %s\n", sessionName)
	} else {
		fmt.Printf("Committed bulk import session: %s\n", sessionName)
	}
}

func handleBulkImportPerform(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Session name required")
		fmt.Println("Usage: tdcli import perform <session_name>")
		os.Exit(1)
	}

	sessionName := args[0]

	if flags.Verbose {
		fmt.Printf("Performing bulk import for session: %s...\n", sessionName)
	}

	job, err := client.BulkImport.Perform(ctx, sessionName)
	handleError(err, "Failed to perform bulk import", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully started bulk import job for session: %s\n", sessionName)
		fmt.Printf("Job ID: %s\n", job.JobID)
	} else {
		fmt.Printf("Started bulk import job: %s\n", job.JobID)
	}
}

func handleBulkImportFreeze(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Session name required")
		fmt.Println("Usage: tdcli import freeze <session_name>")
		os.Exit(1)
	}

	sessionName := args[0]

	err := client.BulkImport.Freeze(ctx, sessionName)
	handleError(err, "Failed to freeze bulk import session", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully froze bulk import session: %s\n", sessionName)
	} else {
		fmt.Printf("Froze bulk import session: %s\n", sessionName)
	}
}

func handleBulkImportUnfreeze(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Session name required")
		fmt.Println("Usage: tdcli import unfreeze <session_name>")
		os.Exit(1)
	}

	sessionName := args[0]

	err := client.BulkImport.Unfreeze(ctx, sessionName)
	handleError(err, "Failed to unfreeze bulk import session", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully unfroze bulk import session: %s\n", sessionName)
	} else {
		fmt.Printf("Unfroze bulk import session: %s\n", sessionName)
	}
}

func handleBulkImportParts(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Session name required")
		fmt.Println("Usage: tdcli import parts <session_name>")
		os.Exit(1)
	}

	sessionName := args[0]
	parts, err := client.BulkImport.ListParts(ctx, sessionName)
	handleError(err, "Failed to list bulk import parts", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(parts)
	case "csv":
		printBulkImportPartsCSV(parts)
	default:
		printBulkImportPartsTable(parts, sessionName)
	}
}

// Print functions
func printBulkImportsTable(bulkImports []td.BulkImport) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tDATABASE\tTABLE\tSTATUS\tVALID_RECORDS\tERROR_RECORDS\tCREATED")

	for _, bi := range bulkImports {
		createdAt := formatTDTime(bi.CreatedAt)

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
			bi.Name,
			bi.Database,
			bi.Table,
			bi.Status,
			bi.ValidRecords,
			bi.ErrorRecords,
			createdAt,
		)
	}
	w.Flush()
}

func printBulkImportDetails(bulkImport td.BulkImport) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PROPERTY\tVALUE")
	fmt.Fprintf(w, "Name\t%s\n", bulkImport.Name)
	fmt.Fprintf(w, "Database\t%s\n", bulkImport.Database)
	fmt.Fprintf(w, "Table\t%s\n", bulkImport.Table)
	fmt.Fprintf(w, "Status\t%s\n", bulkImport.Status)
	fmt.Fprintf(w, "Job ID\t%s\n", bulkImport.JobID)
	fmt.Fprintf(w, "Valid Records\t%d\n", bulkImport.ValidRecords)
	fmt.Fprintf(w, "Error Records\t%d\n", bulkImport.ErrorRecords)
	fmt.Fprintf(w, "Valid Parts\t%d\n", bulkImport.ValidParts)
	fmt.Fprintf(w, "Error Parts\t%d\n", bulkImport.ErrorParts)
	fmt.Fprintf(w, "Upload Frozen\t%t\n", bulkImport.UploadFrozen)
	fmt.Fprintf(w, "Created\t%s\n", formatTDTime(bulkImport.CreatedAt))
	w.Flush()
}

func printBulkImportsCSV(bulkImports []td.BulkImport) {
	fmt.Println("name,database,table,status,valid_records,error_records,valid_parts,error_parts,upload_frozen,created")
	for _, bi := range bulkImports {
		fmt.Printf("%s,%s,%s,%s,%d,%d,%d,%d,%t,%s\n",
			bi.Name,
			bi.Database,
			bi.Table,
			bi.Status,
			bi.ValidRecords,
			bi.ErrorRecords,
			bi.ValidParts,
			bi.ErrorParts,
			bi.UploadFrozen,
			formatTDTime(bi.CreatedAt),
		)
	}
}

func printBulkImportPartsTable(parts []td.BulkImportPart, sessionName string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "SESSION: %s\n\n", sessionName)
	fmt.Fprintln(w, "PART_NAME\tSIZE")

	for _, part := range parts {
		size := formatBytes(part.Size)
		fmt.Fprintf(w, "%s\t%s\n", part.Name, size)
	}
	w.Flush()
}

func printBulkImportPartsCSV(parts []td.BulkImportPart) {
	fmt.Println("part_name,size_bytes")
	for _, part := range parts {
		fmt.Printf("%s,%d\n", part.Name, part.Size)
	}
}
