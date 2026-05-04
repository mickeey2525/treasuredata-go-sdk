package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleBulkImportList(ctx context.Context, client *td.Client, flags Flags) error {
	bulkImports, err := client.BulkImport.List(ctx)
	if err != nil {
		return wrapErr(err, "failed to list bulk import sessions", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(bulkImports)
	case "csv":
		printBulkImportsCSV(bulkImports)
	default:
		printBulkImportsTable(bulkImports)
	}
	return nil
}

func handleBulkImportGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("session name required\nUsage: tdcli import get <session_name>")
	}

	sessionName := args[0]
	bulkImport, err := client.BulkImport.Show(ctx, sessionName)
	if err != nil {
		return wrapErr(err, "failed to get bulk import session", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(bulkImport)
	case "csv":
		printBulkImportsCSV([]td.BulkImport{*bulkImport})
	default:
		printBulkImportDetails(*bulkImport)
	}
	return nil
}

func handleBulkImportCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return errors.New("session name, database, and table required\nUsage: tdcli import create <session_name> <database> <table>")
	}

	sessionName := args[0]
	database := args[1]
	table := args[2]

	if err := client.BulkImport.Create(ctx, sessionName, database, table); err != nil {
		return wrapErr(err, "failed to create bulk import session", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully created bulk import session: %s for %s.%s\n", sessionName, database, table)
	} else {
		fmt.Printf("Created bulk import session: %s\n", sessionName)
	}
	return nil
}

func handleBulkImportDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("session name required\nUsage: tdcli import delete <session_name>")
	}

	sessionName := args[0]

	fmt.Printf("Are you sure you want to delete bulk import session '%s'? (y/N): ", sessionName)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return nil
	}

	if err := client.BulkImport.Delete(ctx, sessionName); err != nil {
		return wrapErr(err, "failed to delete bulk import session", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully deleted bulk import session: %s\n", sessionName)
	} else {
		fmt.Printf("Deleted bulk import session: %s\n", sessionName)
	}
	return nil
}

func handleBulkImportUpload(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return errors.New("session name, part name, and file path required\nUsage: tdcli import upload <session_name> <part_name> <file_path>")
	}

	sessionName := args[0]
	partName := args[1]
	filePath := args[2]

	file, err := os.Open(filePath)
	if err != nil {
		return wrapErr(err, "failed to open file", flags.Verbose)
	}
	defer file.Close()

	if flags.Verbose {
		fmt.Printf("Uploading file %s as part %s to session %s...\n", filePath, partName, sessionName)
	}

	if err := client.BulkImport.UploadPart(ctx, sessionName, partName, file); err != nil {
		return wrapErr(err, "failed to upload part", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully uploaded part %s to session %s\n", partName, sessionName)
	} else {
		fmt.Printf("Uploaded part: %s\n", partName)
	}
	return nil
}

func handleBulkImportCommit(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("session name required\nUsage: tdcli import commit <session_name>")
	}

	sessionName := args[0]

	fmt.Printf("Are you sure you want to commit bulk import session '%s'? (y/N): ", sessionName)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Commit cancelled")
		return nil
	}

	if err := client.BulkImport.Commit(ctx, sessionName); err != nil {
		return wrapErr(err, "failed to commit bulk import session", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully committed bulk import session: %s\n", sessionName)
	} else {
		fmt.Printf("Committed bulk import session: %s\n", sessionName)
	}
	return nil
}

func handleBulkImportPerform(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("session name required\nUsage: tdcli import perform <session_name>")
	}

	sessionName := args[0]

	if flags.Verbose {
		fmt.Printf("Performing bulk import for session: %s...\n", sessionName)
	}

	job, err := client.BulkImport.Perform(ctx, sessionName)
	if err != nil {
		return wrapErr(err, "failed to perform bulk import", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully started bulk import job for session: %s\n", sessionName)
		fmt.Printf("Job ID: %s\n", job.JobID)
	} else {
		fmt.Printf("Started bulk import job: %s\n", job.JobID)
	}
	return nil
}

func handleBulkImportFreeze(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("session name required\nUsage: tdcli import freeze <session_name>")
	}

	sessionName := args[0]

	if err := client.BulkImport.Freeze(ctx, sessionName); err != nil {
		return wrapErr(err, "failed to freeze bulk import session", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully froze bulk import session: %s\n", sessionName)
	} else {
		fmt.Printf("Froze bulk import session: %s\n", sessionName)
	}
	return nil
}

func handleBulkImportUnfreeze(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("session name required\nUsage: tdcli import unfreeze <session_name>")
	}

	sessionName := args[0]

	if err := client.BulkImport.Unfreeze(ctx, sessionName); err != nil {
		return wrapErr(err, "failed to unfreeze bulk import session", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully unfroze bulk import session: %s\n", sessionName)
	} else {
		fmt.Printf("Unfroze bulk import session: %s\n", sessionName)
	}
	return nil
}

func handleBulkImportParts(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("session name required\nUsage: tdcli import parts <session_name>")
	}

	sessionName := args[0]
	parts, err := client.BulkImport.ListParts(ctx, sessionName)
	if err != nil {
		return wrapErr(err, "failed to list bulk import parts", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(parts)
	case "csv":
		printBulkImportPartsCSV(parts)
	default:
		printBulkImportPartsTable(parts, sessionName)
	}
	return nil
}

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
