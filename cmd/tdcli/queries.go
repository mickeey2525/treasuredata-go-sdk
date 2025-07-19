package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleQueryCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printQueryUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "submit", "run":
		handleQuerySubmit(ctx, client, subArgs, flags)
	case "status":
		handleQueryStatus(ctx, client, subArgs, flags)
	case "result", "results":
		handleQueryResult(ctx, client, subArgs, flags)
	case "list", "ls":
		handleQueryList(ctx, client, flags)
	case "cancel":
		handleQueryCancel(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown query subcommand: %s\n", subcommand)
		printQueryUsage()
		os.Exit(1)
	}
}

func printQueryUsage() {
	fmt.Printf(`Query Execution Commands

USAGE:
    tdcli queries <subcommand> [options]
    tdcli query <subcommand> [options]
    tdcli q <subcommand> [options]

SUBCOMMANDS:
    submit, run <query>    Submit a query for execution
    status <job_id>        Check query execution status
    result, results <job_id> Get query results
    list, ls               List recent queries
    cancel <job_id>        Cancel a running query

OPTIONS:
    --database DATABASE    Database to run query against (required for submit)
    --engine ENGINE        Query engine: trino (default) or hive
    --priority PRIORITY    Query priority (0-2, default: 0)
    --result-url URL       Result output URL
    --type TYPE            Result format type
    --wait                 Wait for query completion
    --timeout SECONDS      Wait timeout in seconds (default: 300)
    --format FORMAT        Output format (json, table, csv)
    --limit LIMIT          Limit number of result rows
    --verbose, -v          Verbose output

EXAMPLES:
    tdcli q submit "SELECT COUNT(*) FROM my_table" --database my_db
    tdcli q submit "SELECT * FROM users LIMIT 10" --database analytics --wait
    tdcli q status 12345
    tdcli q result 12345 --format csv
    tdcli q list
    tdcli q cancel 12345

`)
}

func handleQuerySubmit(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Query string required")
		fmt.Println("Usage: tdcli q submit \"<query>\" --database <database>")
		os.Exit(1)
	}

	query := args[0]
	database := flags.Database
	var engine td.QueryType

	if database == "" {
		fmt.Println("Error: Database name required")
		fmt.Println("Usage: tdcli q submit \"<query>\" --database <database>")
		os.Exit(1)
	}

	// Determine query engine - check flag first, then env var, then default
	if flags.Engine != "" {
		switch strings.ToLower(flags.Engine) {
		case "hive":
			engine = td.QueryTypeHive
		case "presto":
			engine = td.QueryTypePresto
		case "trino":
			engine = td.QueryTypeTrino
		default:
			engine = td.QueryTypeTrino
		}
	} else {
		switch strings.ToLower(os.Getenv("TD_QUERY_ENGINE")) {
		case "hive":
			engine = td.QueryTypeHive
		case "presto":
			engine = td.QueryTypePresto
		default:
			engine = td.QueryTypeTrino // Default to Trino
		}
	}

	opts := &td.IssueQueryOptions{
		Query: query,
	}
	if flags.Priority > 0 {
		opts.Priority = flags.Priority
	}

	if flags.Verbose {
		fmt.Printf("Submitting query to database: %s\n", database)
		fmt.Printf("Query engine: %s\n", engine)
		fmt.Printf("Query: %s\n", query)
	}

	job, err := client.Queries.Issue(ctx, engine, database, opts)
	handleError(err, "Failed to submit query", flags.Verbose)

	fmt.Printf("Query submitted successfully\n")
	fmt.Printf("Job ID: %s\n", job.JobID)

	// If wait flag is set, wait for completion
	if os.Getenv("TD_WAIT") == "true" || containsFlag(os.Args, "--wait") {
		handleQueryWait(ctx, client, job.JobID, flags)
	}
}

func handleQueryStatus(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Job ID required")
		fmt.Println("Usage: tdcli q status <job_id>")
		os.Exit(1)
	}

	jobIDStr := args[0]

	job, err := client.Jobs.Get(ctx, jobIDStr)
	handleError(err, "Failed to get job status", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(job)
	default:
		printJobDetails(*job)
	}
}

func handleQueryResult(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Job ID required")
		fmt.Println("Usage: tdcli q result <job_id>")
		os.Exit(1)
	}

	jobIDStr := args[0]

	// First check if job is completed
	job, err := client.Jobs.Get(ctx, jobIDStr)
	handleError(err, "Failed to get job status", flags.Verbose)

	if job.Status != "success" {
		fmt.Printf("Job status: %s\n", job.Status)
		if job.Status == "error" && job.Debug != nil {
			fmt.Printf("Error: %s\n", job.Debug.Stderr)
		}
		return
	}

	// Get results
	format := "json" // Default format for API
	if flags.Format == "csv" {
		format = "csv"
	}

	opts := &td.GetResultOptions{
		Format: td.ResultFormat(format),
	}
	if flags.Limit > 0 {
		opts.Limit = flags.Limit
	}
	resultReader, err := client.Results.GetResult(ctx, jobIDStr, opts)
	handleError(err, "Failed to get query results", flags.Verbose)
	defer resultReader.Close()

	// Read all results
	resultsBytes, err := io.ReadAll(resultReader)
	handleError(err, "Failed to read query results", flags.Verbose)
	results := string(resultsBytes)

	switch flags.Format {
	case "json":
		printJSON(results)
	case "csv":
		fmt.Print(results)
	default:
		// Try to format as table if it's JSON
		if format == "json" {
			printQueryResultsTable(results, flags.Limit)
		} else {
			fmt.Print(results)
		}
	}
}

func handleQueryList(ctx context.Context, client *td.Client, flags Flags) {
	var opts *td.JobListOptions
	if flags.Status != "" {
		opts = &td.JobListOptions{Status: flags.Status}
	}

	jobsResp, err := client.Jobs.List(ctx, opts)
	handleError(err, "Failed to list jobs", flags.Verbose)
	jobs := jobsResp.Jobs

	// Filter to only queries (not other job types)
	var queryJobs []td.Job
	for _, job := range jobs {
		if job.Type == "trino" || job.Type == "hive" || job.Type == "presto" {
			queryJobs = append(queryJobs, job)
		}
	}

	switch flags.Format {
	case "json":
		printJSON(queryJobs)
	case "csv":
		printJobsCSV(queryJobs)
	default:
		printJobsTable(queryJobs)
	}
}

func handleQueryCancel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Job ID required")
		fmt.Println("Usage: tdcli q cancel <job_id>")
		os.Exit(1)
	}

	jobIDStr := args[0]

	err := client.Jobs.Kill(ctx, jobIDStr)
	handleError(err, "Failed to cancel job", flags.Verbose)

	fmt.Printf("Job %s cancelled\n", jobIDStr)
}

func handleQueryWait(ctx context.Context, client *td.Client, jobID string, flags Flags) {
	timeout := 300 // Default 5 minutes
	if timeoutEnv := os.Getenv("TD_TIMEOUT"); timeoutEnv != "" {
		if t, err := strconv.Atoi(timeoutEnv); err == nil {
			timeout = t
		}
	}

	fmt.Printf("Waiting for job %s to complete (timeout: %ds)...\n", jobID, timeout)

	start := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context cancelled")
			return
		case <-ticker.C:
			if time.Since(start).Seconds() > float64(timeout) {
				fmt.Printf("Timeout waiting for job %s\n", jobID)
				return
			}

			job, err := client.Jobs.Get(ctx, jobID)
			if err != nil {
				fmt.Printf("Error checking job status: %v\n", err)
				return
			}

			switch job.Status {
			case "success":
				fmt.Printf("Job %s completed successfully\n", jobID)
				if flags.Verbose {
					printJobDetails(*job)
				}
				return
			case "error":
				fmt.Printf("Job %s failed\n", jobID)
				if job.Debug != nil && job.Debug.Stderr != "" {
					fmt.Printf("Error: %s\n", job.Debug.Stderr)
				}
				return
			case "killed":
				fmt.Printf("Job %s was cancelled\n", jobID)
				return
			default:
				if flags.Verbose {
					fmt.Printf("Job %s status: %s\n", jobID, job.Status)
				}
			}
		}
	}
}

func printQueryResultsTable(results interface{}, limit int) {
	// This is a simplified table printer for query results
	// In a real implementation, you'd parse the JSON results properly
	fmt.Printf("Query Results:\n")
	fmt.Printf("%v\n", results)

	if limit > 0 {
		fmt.Printf("(Limited to %d rows)\n", limit)
	}
}

func containsFlag(args []string, flag string) bool {
	for _, arg := range args {
		if arg == flag {
			return true
		}
	}
	return false
}
