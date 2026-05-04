package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleTableList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	var database string

	if flags.Database != "" {
		database = flags.Database
	} else if len(args) > 0 {
		database = args[0]
	} else {
		return errors.New("database name required\nUsage: tdcli table list <database> OR tdcli table list --database <database>")
	}

	tables, err := client.Tables.List(ctx, database)
	if err != nil {
		return wrapErr(err, "failed to list tables", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(tables)
	case "csv":
		printTablesCSV(tables)
	default:
		printTablesTable(tables, database)
	}
	return nil
}

func handleTableGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return errors.New("database and table names required\nUsage: tdcli table get <database> <table>")
	}

	database := args[0]
	tableName := args[1]

	table, err := client.Tables.Get(ctx, database, tableName)
	if err != nil {
		return wrapErr(err, "failed to get table", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(table)
	case "csv":
		printTablesCSV([]td.Table{*table})
	default:
		printTableDetails(*table)
	}
	return nil
}

func handleTableCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return errors.New("database and table names required\nUsage: tdcli table create <database> <table>")
	}

	database := args[0]
	tableName := args[1]

	table, err := client.Tables.Create(ctx, database, tableName, "")
	if err != nil {
		return wrapErr(err, "failed to create table", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully created table: %s.%s\n", database, table.Table)
	}

	switch flags.Format {
	case "json":
		printJSON(table)
	default:
		fmt.Printf("Created table: %s.%s\n", database, table.Table)
	}
	return nil
}

func handleTableDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return errors.New("database and table names required\nUsage: tdcli table delete <database> <table>")
	}

	database := args[0]
	tableName := args[1]

	fmt.Printf("Are you sure you want to delete table '%s.%s'? (y/N): ", database, tableName)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return nil
	}

	if err := client.Tables.Delete(ctx, database, tableName); err != nil {
		return wrapErr(err, "failed to delete table", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully deleted table: %s.%s\n", database, tableName)
	} else {
		fmt.Printf("Deleted table: %s.%s\n", database, tableName)
	}
	return nil
}

func handleTableSwap(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return errors.New("database and two table names required\nUsage: tdcli table swap <database> <table1> <table2>")
	}

	database := args[0]
	table1 := args[1]
	table2 := args[2]

	fmt.Printf("Are you sure you want to swap tables '%s.%s' and '%s.%s'? (y/N): ",
		database, table1, database, table2)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Swap cancelled")
		return nil
	}

	if err := client.Tables.Swap(ctx, database, table1, table2); err != nil {
		return wrapErr(err, "failed to swap tables", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully initiated table swap: %s.%s <-> %s.%s\n",
			database, table1, database, table2)
	} else {
		fmt.Printf("Swapped tables: %s.%s <-> %s.%s\n",
			database, table1, database, table2)
	}
	return nil
}

func handleTableRename(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return errors.New("database, old name, and new name required\nUsage: tdcli table rename <database> <old_name> <new_name>")
	}

	database := args[0]
	oldName := args[1]
	newName := args[2]

	fmt.Printf("Are you sure you want to rename table '%s.%s' to '%s.%s'? (y/N): ",
		database, oldName, database, newName)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Rename cancelled")
		return nil
	}

	if err := client.Tables.Rename(ctx, database, oldName, newName); err != nil {
		return wrapErr(err, "failed to rename table", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully renamed table: %s.%s -> %s.%s\n",
			database, oldName, database, newName)
	} else {
		fmt.Printf("Renamed table: %s.%s -> %s.%s\n",
			database, oldName, database, newName)
	}
	return nil
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
