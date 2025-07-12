package main

import (
	"context"
	"fmt"
	"io"
	"os"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleResultCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printResultUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "get", "show":
		handleResultGet(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown result subcommand: %s\n", subcommand)
		printResultUsage()
		os.Exit(1)
	}
}

func printResultUsage() {
	fmt.Printf(`Result Management Commands

USAGE:
    tdcli results <subcommand> [options]
    tdcli result <subcommand> [options]

SUBCOMMANDS:
    get, show <job_id>     Get query results

OPTIONS:
    --format FORMAT        Result format (json, csv, tsv, jsonl)
    --limit LIMIT          Limit number of rows
    --output FILE          Save results to file
    --verbose, -v          Verbose output

EXAMPLES:
    tdcli result get 12345
    tdcli result get 12345 --format csv
    tdcli result get 12345 --limit 100 --output results.csv

`)
}

func handleResultGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Job ID required")
		fmt.Println("Usage: tdcli result get <job_id>")
		os.Exit(1)
	}

	jobIDStr := args[0]

	// Set up result options
	opts := &td.GetResultOptions{}

	// Set format
	format := flags.Format
	if format == "" || format == "table" {
		format = "json" // Default to JSON for API
	}
	opts.Format = td.ResultFormat(format)

	// Set limit
	if flags.Limit > 0 {
		opts.Limit = flags.Limit
	}

	if flags.Verbose {
		fmt.Printf("Getting results for job: %s\n", jobIDStr)
		fmt.Printf("Format: %s\n", format)
		if flags.Limit > 0 {
			fmt.Printf("Limit: %d\n", flags.Limit)
		}
	}

	// Get results
	resultReader, err := client.Results.GetResult(ctx, jobIDStr, opts)
	handleError(err, "Failed to get results", flags.Verbose)
	defer resultReader.Close()

	// Read all results
	resultsBytes, err := io.ReadAll(resultReader)
	handleError(err, "Failed to read results", flags.Verbose)

	results := string(resultsBytes)

	// Output results
	if flags.Output != "" {
		err = writeOutput(results, flags.Output)
		handleError(err, "Failed to write results to file", flags.Verbose)
		fmt.Printf("Results saved to: %s\n", flags.Output)
	} else {
		fmt.Print(results)
	}
}
