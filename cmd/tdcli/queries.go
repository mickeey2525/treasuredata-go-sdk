package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleQuerySubmit(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("query string required\nUsage: tdcli q submit \"<query>\" --database <database>")
	}

	query := args[0]
	database := flags.Database
	var engine td.QueryType

	if database == "" {
		return errors.New("database name required\nUsage: tdcli q submit \"<query>\" --database <database>")
	}

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
			engine = td.QueryTypeTrino
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
	if err != nil {
		return wrapErr(err, "failed to submit query", flags.Verbose)
	}

	fmt.Printf("Query submitted successfully\n")
	fmt.Printf("Job ID: %s\n", job.JobID)

	if os.Getenv("TD_WAIT") == "true" || containsFlag(os.Args, "--wait") {
		return handleQueryWait(ctx, client, job.JobID, flags)
	}
	return nil
}

func handleQueryStatus(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("job ID required\nUsage: tdcli q status <job_id>")
	}

	job, err := client.Jobs.Get(ctx, args[0])
	if err != nil {
		return wrapErr(err, "failed to get job status", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(job)
	default:
		printJobDetails(*job)
	}
	return nil
}

func handleQueryResult(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("job ID required\nUsage: tdcli q result <job_id>")
	}

	jobIDStr := args[0]

	job, err := client.Jobs.Get(ctx, jobIDStr)
	if err != nil {
		return wrapErr(err, "failed to get job status", flags.Verbose)
	}

	if job.Status != "success" {
		fmt.Printf("Job status: %s\n", job.Status)
		if job.Status == "error" && job.Debug != nil {
			fmt.Printf("Error: %s\n", job.Debug.Stderr)
		}
		return nil
	}

	format := "json"
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
	if err != nil {
		return wrapErr(err, "failed to get query results", flags.Verbose)
	}
	defer resultReader.Close()

	resultsBytes, err := io.ReadAll(resultReader)
	if err != nil {
		return wrapErr(err, "failed to read query results", flags.Verbose)
	}
	results := string(resultsBytes)

	switch flags.Format {
	case "json":
		printJSON(results)
	case "csv":
		fmt.Print(results)
	default:
		if format == "json" {
			printQueryResultsTable(results, flags.Limit)
		} else {
			fmt.Print(results)
		}
	}
	return nil
}

func handleQueryList(ctx context.Context, client *td.Client, flags Flags) error {
	var opts *td.JobListOptions
	if flags.Status != "" {
		opts = &td.JobListOptions{Status: flags.Status}
	}

	jobsResp, err := client.Jobs.List(ctx, opts)
	if err != nil {
		return wrapErr(err, "failed to list jobs", flags.Verbose)
	}
	jobs := jobsResp.Jobs

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
	return nil
}

func handleQueryCancel(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("job ID required\nUsage: tdcli q cancel <job_id>")
	}

	jobIDStr := args[0]

	if err := client.Jobs.Kill(ctx, jobIDStr); err != nil {
		return wrapErr(err, "failed to cancel job", flags.Verbose)
	}

	fmt.Printf("Job %s cancelled\n", jobIDStr)
	return nil
}

func handleQueryWait(ctx context.Context, client *td.Client, jobID string, flags Flags) error {
	timeout := 300
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
			return ctx.Err()
		case <-ticker.C:
			if time.Since(start).Seconds() > float64(timeout) {
				return fmt.Errorf("timeout waiting for job %s", jobID)
			}

			job, err := client.Jobs.Get(ctx, jobID)
			if err != nil {
				return wrapErr(err, "failed to check job status", flags.Verbose)
			}

			switch job.Status {
			case "success":
				fmt.Printf("Job %s completed successfully\n", jobID)
				if flags.Verbose {
					printJobDetails(*job)
				}
				return nil
			case "error":
				fmt.Printf("Job %s failed\n", jobID)
				if job.Debug != nil && job.Debug.Stderr != "" {
					fmt.Printf("Error: %s\n", job.Debug.Stderr)
				}
				return fmt.Errorf("job %s failed", jobID)
			case "killed":
				fmt.Printf("Job %s was cancelled\n", jobID)
				return fmt.Errorf("job %s was cancelled", jobID)
			default:
				if flags.Verbose {
					fmt.Printf("Job %s status: %s\n", jobID, job.Status)
				}
			}
		}
	}
}

func printQueryResultsTable(results interface{}, limit int) {
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
