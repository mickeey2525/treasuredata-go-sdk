package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleDatabaseList(ctx context.Context, client *td.Client, flags Flags) error {
	databases, err := client.Databases.List(ctx)
	if err != nil {
		return wrapErr(err, "failed to list databases", flags.Verbose)
	}

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
		return wrapErr(err, "failed to write output", flags.Verbose)
	}
	return nil
}

func handleDatabaseGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("database name required\nUsage: tdcli db get <database_name>")
	}

	name := args[0]
	database, err := client.Databases.Get(ctx, name)
	if err != nil {
		return wrapErr(err, "failed to get database", flags.Verbose)
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
		return wrapErr(err, "failed to write output", flags.Verbose)
	}
	return nil
}

func handleDatabaseCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("database name required\nUsage: tdcli db create <database_name>")
	}

	name := args[0]
	database, err := client.Databases.Create(ctx, name)
	if err != nil {
		return wrapErr(err, "failed to create database", flags.Verbose)
	}

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
		return wrapErr(err, "failed to write output", flags.Verbose)
	}
	return nil
}

func handleDatabaseDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("database name required\nUsage: tdcli db delete <database_name>")
	}

	name := args[0]

	fmt.Printf("Are you sure you want to delete database '%s'? (y/N): ", name)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return nil
	}

	if err := client.Databases.Delete(ctx, name); err != nil {
		return wrapErr(err, "failed to delete database", flags.Verbose)
	}

	if flags.Verbose {
		fmt.Printf("Successfully deleted database: %s\n", name)
	} else {
		fmt.Printf("Deleted database: %s\n", name)
	}
	return nil
}

func handleDatabaseUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("database name required\nUsage: tdcli db update <database_name> --permission <permission>")
	}

	name := args[0]

	database, err := client.Databases.Get(ctx, name)
	if err != nil {
		return wrapErr(err, "failed to get database for update", flags.Verbose)
	}

	fmt.Printf("Database update functionality would be implemented here for: %s\n", database.Name)
	fmt.Println("Note: Check Treasure Data API documentation for updateable database properties")
	return nil
}
