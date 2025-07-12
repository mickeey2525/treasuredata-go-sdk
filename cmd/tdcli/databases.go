package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleDatabaseCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printDatabaseUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "list", "ls":
		handleDatabaseList(ctx, client, flags)
	case "get", "show":
		handleDatabaseGet(ctx, client, subArgs, flags)
	case "create":
		handleDatabaseCreate(ctx, client, subArgs, flags)
	case "delete", "rm":
		handleDatabaseDelete(ctx, client, subArgs, flags)
	case "update":
		handleDatabaseUpdate(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown database subcommand: %s\n", subcommand)
		printDatabaseUsage()
		os.Exit(1)
	}
}

func printDatabaseUsage() {
	fmt.Printf(`Database Management Commands

USAGE:
    tdcli databases <subcommand> [options]
    tdcli db <subcommand> [options]

SUBCOMMANDS:
    list, ls              List all databases
    get, show <name>      Get database details
    create <name>         Create a new database
    delete, rm <name>     Delete a database
    update <name>         Update database properties

OPTIONS:
    --format FORMAT       Output format (json, table, csv)
    --verbose, -v         Verbose output

EXAMPLES:
    tdcli db list
    tdcli db show my_database
    tdcli db create new_database
    tdcli db delete old_database
    tdcli db update my_database --permission full

`)
}

func handleDatabaseList(ctx context.Context, client *td.Client, flags Flags) {
	databases, err := client.Databases.List(ctx)
	handleError(err, "Failed to list databases", flags.Verbose)

	csvFormatter := func(data interface{}) string {
		databases := data.([]td.Database)
		var csvBuilder strings.Builder
		for _, db := range databases {
			csvBuilder.WriteString(fmt.Sprintf("%s,%d,%s,%s,%s\n",
				db.Name,
				db.Count,
				formatTDTime(db.CreatedAt),
				formatTDTime(db.UpdatedAt),
				db.Permission,
			))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		databases := data.([]td.Database)
		if len(databases) == 0 {
			return "No databases found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tTABLES\tCREATED\tUPDATED\tPERMISSION")
		for _, db := range databases {
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\n",
				db.Name,
				db.Count,
				formatTDTime(db.CreatedAt),
				formatTDTime(db.UpdatedAt),
				db.Permission,
			)
		}
		w.Flush()
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(databases, flags.Format, flags.Output, "name,tables,created,updated,permission", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

func handleDatabaseGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Database name required")
		fmt.Println("Usage: tdcli db get <database_name>")
		os.Exit(1)
	}

	name := args[0]
	database, err := client.Databases.Get(ctx, name)
	handleError(err, "Failed to get database", flags.Verbose)

	csvFormatter := func(data interface{}) string {
		db := data.(*td.Database)
		return fmt.Sprintf("%s,%d,%s,%s,%s\n",
			db.Name,
			db.Count,
			formatTDTime(db.CreatedAt),
			formatTDTime(db.UpdatedAt),
			db.Permission,
		)
	}

	tableFormatter := func(data interface{}) string {
		db := data.(*td.Database)
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "PROPERTY\tVALUE")
		fmt.Fprintf(w, "Name\t%s\n", db.Name)
		fmt.Fprintf(w, "Tables\t%d\n", db.Count)
		fmt.Fprintf(w, "Created\t%s\n", formatTDTime(db.CreatedAt))
		fmt.Fprintf(w, "Updated\t%s\n", formatTDTime(db.UpdatedAt))
		fmt.Fprintf(w, "Permission\t%s\n", db.Permission)
		w.Flush()
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(database, flags.Format, flags.Output, "name,tables,created,updated,permission", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

func handleDatabaseCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Database name required")
		fmt.Println("Usage: tdcli db create <database_name>")
		os.Exit(1)
	}

	name := args[0]
	database, err := client.Databases.Create(ctx, name)
	handleError(err, "Failed to create database", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully created database: %s\n", database.Name)
	}

	csvFormatter := func(data interface{}) string {
		db := data.(*td.Database)
		return fmt.Sprintf("%s,%d,%s,%s,%s\n",
			db.Name,
			db.Count,
			formatTDTime(db.CreatedAt),
			formatTDTime(db.UpdatedAt),
			db.Permission,
		)
	}

	tableFormatter := func(data interface{}) string {
		db := data.(*td.Database)
		return fmt.Sprintf("Created database: %s\n", db.Name)
	}

	if err := formatAndWriteOutput(database, flags.Format, flags.Output, "name,tables,created,updated,permission", csvFormatter, tableFormatter); err != nil {
		handleError(err, "Failed to write output", flags.Verbose)
	}
}

func handleDatabaseDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Database name required")
		fmt.Println("Usage: tdcli db delete <database_name>")
		os.Exit(1)
	}

	name := args[0]

	// Confirm deletion
	fmt.Printf("Are you sure you want to delete database '%s'? (y/N): ", name)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return
	}

	err := client.Databases.Delete(ctx, name)
	handleError(err, "Failed to delete database", flags.Verbose)

	if flags.Verbose {
		fmt.Printf("Successfully deleted database: %s\n", name)
	} else {
		fmt.Printf("Deleted database: %s\n", name)
	}
}

func handleDatabaseUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Database name required")
		fmt.Println("Usage: tdcli db update <database_name> --permission <permission>")
		os.Exit(1)
	}

	name := args[0]

	// For now, we'll just get and display the database
	// The actual update functionality would depend on what database properties can be updated
	database, err := client.Databases.Get(ctx, name)
	handleError(err, "Failed to get database for update", flags.Verbose)

	fmt.Printf("Database update functionality would be implemented here for: %s\n", database.Name)
	fmt.Println("Note: Check Treasure Data API documentation for updateable database properties")
}

