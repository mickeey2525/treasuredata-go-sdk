package main

import (
	"context"
	"fmt"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleStreamImport(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		fmt.Println("Usage: tdcli stream import <database> <table> <file-path> [unique-id]")
		fmt.Println("Example: tdcli stream import mydb mytable data.msgpack.gz")
		return
	}

	database := args[0]
	table := args[1]
	filePath := args[2]
	uniqueID := ""
	if len(args) > 3 {
		uniqueID = args[3]
	}

	file, err := os.Open(filePath)
	if err != nil {
		handleError(err, "Failed to open import file", flags.Verbose)
		return
	}
	defer file.Close()

	var resp *td.ImportResponse
	if uniqueID != "" {
		resp, err = client.StreamImport.ImportTableWithID(ctx, database, table, uniqueID, file)
	} else {
		resp, err = client.StreamImport.ImportTable(ctx, database, table, file)
	}

	if err != nil {
		handleError(err, "Failed to import data", flags.Verbose)
		return
	}

	fmt.Printf("Import completed successfully\n")
	if resp != nil && resp.Status != "" {
		fmt.Printf("Status: %s\n", resp.Status)
	}
}
