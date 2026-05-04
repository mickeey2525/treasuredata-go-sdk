package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

const streamImportSizeWarningThreshold = 100 * 1024 * 1024 // 100 MB

func handleStreamImport(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	if len(args) < 3 {
		return errors.New("usage: tdcli stream import <database> <table> <file-path> [unique-id]\n" +
			"Example: tdcli stream import mydb mytable data.msgpack.gz")
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
		return wrapErr(err, "failed to open import file", flags.Verbose)
	}
	defer file.Close()

	if info, err := file.Stat(); err == nil {
		if info.Size() > streamImportSizeWarningThreshold {
			fmt.Printf("Warning: file size is %d MB (>100 MB). Large files may take a while or fail due to network timeouts.\n", info.Size()/(1024*1024))
		}
	}

	var resp *td.ImportResponse
	if uniqueID != "" {
		resp, err = client.StreamImport.ImportTableWithID(ctx, database, table, uniqueID, file)
	} else {
		resp, err = client.StreamImport.ImportTable(ctx, database, table, file)
	}

	if err != nil {
		return wrapErr(err, "failed to import data", flags.Verbose)
	}

	fmt.Printf("Import completed successfully\n")
	if resp != nil && resp.Status != "" {
		fmt.Printf("Status: %s\n", resp.Status)
	}
	return nil
}
