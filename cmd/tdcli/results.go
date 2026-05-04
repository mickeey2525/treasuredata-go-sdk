package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleResultGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("job ID required\nUsage: tdcli result get <job_id>")
	}

	jobIDStr := args[0]

	opts := &td.GetResultOptions{}

	format := flags.Format
	if format == "" || format == "table" {
		format = "json"
	}
	opts.Format = td.ResultFormat(format)

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

	resultReader, err := client.Results.GetResult(ctx, jobIDStr, opts)
	if err != nil {
		return wrapErr(err, "failed to get results", flags.Verbose)
	}
	defer resultReader.Close()

	resultsBytes, err := io.ReadAll(resultReader)
	if err != nil {
		return wrapErr(err, "failed to read results", flags.Verbose)
	}

	results := string(resultsBytes)

	if flags.Output != "" {
		if err := writeOutput(results, flags.Output); err != nil {
			return wrapErr(err, "failed to write results to file", flags.Verbose)
		}
		fmt.Printf("Results saved to: %s\n", flags.Output)
	} else {
		fmt.Print(results)
	}
	return nil
}
