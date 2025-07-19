package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleTableCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printTableUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "list", "ls":
		handleTableList(ctx, client, subArgs, flags)
	case "get", "show":
		handleTableGet(ctx, client, subArgs, flags)
	case "create":
		handleTableCreate(ctx, client, subArgs, flags)
	case "delete", "rm":
		handleTableDelete(ctx, client, subArgs, flags)
	case "swap":
		handleTableSwap(ctx, client, subArgs, flags)
	case "rename", "mv":
		handleTableRename(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown table subcommand: %s\n", subcommand)
		printTableUsage()
		os.Exit(1)
	}
}

func printTableUsage() {
	fmt.Printf(`Table Management Commands

USAGE:
    tdcli tables <subcommand> [options]
    tdcli table <subcommand> [options]

SUBCOMMANDS:
    list, ls <database>           List tables in database
    get, show <database> <table>  Get table details
    create <database> <table>     Create a new table
    delete, rm <database> <table> Delete a table
    swap <database> <table1> <table2>  Swap two tables
    rename, mv <database> <from> <to>  Rename a table

OPTIONS:
    --database DATABASE   Database name (alternative to positional arg)
    --format FORMAT       Output format (json, table, csv)
    --verbose, -v         Verbose output

EXAMPLES:
    tdcli table list my_database
    tdcli table show my_database my_table
    tdcli table create my_database new_table
    tdcli table delete my_database old_table
    tdcli table swap my_database table1 table2
    tdcli table rename my_database old_name new_name

`)
}

func handleTableList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	var database string

	if flags.Database != "" {
		database = flags.Database
	} else if len(args) > 0 {
		database = args[0]
	} else {
		fmt.Println("Error: Database name required")
		fmt.Println("Usage: tdcli table list <database> OR tdcli table list --database <database>")
		os.Exit(1)
	}

	tables, err := client.Tables.List(ctx, database)
	handleError(err, "Failed to list tables", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(tables)
	case "csv":
		printTablesCSV(tables)
	default:
		printTablesTable(tables, database)
	}
}

func handleTableGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		fmt.Println("Error: Database and table names required")
		fmt.Println("Usage: tdcli table get <database> <table>")
		os.Exit(1)
	}

	database := args[0]
	tableName := args[1]

	table, err := client.Tables.Get(ctx, database, tableName)
	handleError(err, "Failed to get table", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(table)
	case "csv":
		printTablesCSV([]td.Table{*table})
	default:
		printTableDetails(*table)
	}
}

func handleTableCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		fmt.Println("Error: Database and table names required")
		fmt.Println("Usage: tdcli table create <database> <table>")
		os.Exit(1)
	}

	database := args[0]
	tableName := args[1]

	table, err := client.Tables.Create(ctx, database, tableName, "")
	handleError(err, "Failed to create table", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully created table: %s.%s\n", database, table.Table)
	}

	switch flags.Format {
	case "json":
		printJSON(table)
	default:
		fmt.Printf("Created table: %s.%s\n", database, table.Table)
	}
}

func handleTableDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		fmt.Println("Error: Database and table names required")
		fmt.Println("Usage: tdcli table delete <database> <table>")
		os.Exit(1)
	}

	database := args[0]
	tableName := args[1]

	// Confirm deletion
	fmt.Printf("Are you sure you want to delete table '%s.%s'? (y/N): ", database, tableName)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return
	}

	err := client.Tables.Delete(ctx, database, tableName)
	handleError(err, "Failed to delete table", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully deleted table: %s.%s\n", database, tableName)
	} else {
		fmt.Printf("Deleted table: %s.%s\n", database, tableName)
	}
}

func handleTableSwap(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		fmt.Println("Error: Database and two table names required")
		fmt.Println("Usage: tdcli table swap <database> <table1> <table2>")
		os.Exit(1)
	}

	database := args[0]
	table1 := args[1]
	table2 := args[2]

	// Confirm swap
	fmt.Printf("Are you sure you want to swap tables '%s.%s' and '%s.%s'? (y/N): ",
		database, table1, database, table2)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Swap cancelled")
		return
	}

	err := client.Tables.Swap(ctx, database, table1, table2)
	handleError(err, "Failed to swap tables", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully initiated table swap: %s.%s <-> %s.%s\n",
			database, table1, database, table2)
	} else {
		fmt.Printf("Swapped tables: %s.%s <-> %s.%s\n",
			database, table1, database, table2)
	}
}

func handleTableRename(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		fmt.Println("Error: Database, old name, and new name required")
		fmt.Println("Usage: tdcli table rename <database> <old_name> <new_name>")
		os.Exit(1)
	}

	database := args[0]
	oldName := args[1]
	newName := args[2]

	// Confirm rename
	fmt.Printf("Are you sure you want to rename table '%s.%s' to '%s.%s'? (y/N): ",
		database, oldName, database, newName)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Rename cancelled")
		return
	}

	err := client.Tables.Rename(ctx, database, oldName, newName)
	handleError(err, "Failed to rename table", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully renamed table: %s.%s -> %s.%s\n",
			database, oldName, database, newName)
	} else {
		fmt.Printf("Renamed table: %s.%s -> %s.%s\n",
			database, oldName, database, newName)
	}
}

func printTablesTable(tables []td.Table, database string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "DATABASE: %s\n\n", database)
	fmt.Fprintln(w, "NAME\tROWS\tSIZE\tCREATED\tUPDATED\tTYPE")

	for _, table := range tables {
		createdAt := formatTDTime(table.CreatedAt)
		updatedAt := formatTDTime(table.UpdatedAt)

		size := formatBytes(table.EstimatedStorageSize)

		fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%s\n",
			table.Name,
			table.Count,
			size,
			createdAt,
			updatedAt,
			table.Type,
		)
	}
	w.Flush()
}

func printTableDetails(table td.Table) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PROPERTY\tVALUE")
	fmt.Fprintf(w, "Name\t%s\n", table.Name)
	fmt.Fprintf(w, "Database\t%s\n", table.Database)
	fmt.Fprintf(w, "Type\t%s\n", table.Type)
	fmt.Fprintf(w, "Rows\t%d\n", table.Count)
	fmt.Fprintf(w, "Size\t%s\n", formatBytes(table.EstimatedStorageSize))
	fmt.Fprintf(w, "Created\t%s\n", formatTDTime(table.CreatedAt))
	fmt.Fprintf(w, "Updated\t%s\n", formatTDTime(table.UpdatedAt))

	// LastImport field doesn't exist in the current API
	if table.LastLogTimestamp.Value != nil {
		fmt.Fprintf(w, "Last Log\t%d\n", *table.LastLogTimestamp.Value)
	}
	w.Flush()
}

func printTablesCSV(tables []td.Table) {
	fmt.Println("name,database,type,rows,size_bytes,created,updated")
	for _, table := range tables {
		fmt.Printf("%s,%s,%s,%d,%d,%s,%s\n",
			table.Name,
			table.Database,
			table.Type,
			table.Count,
			table.EstimatedStorageSize,
			formatTDTime(table.CreatedAt),
			formatTDTime(table.UpdatedAt),
		)
	}
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
