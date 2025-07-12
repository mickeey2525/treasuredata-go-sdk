package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleJobCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printJobUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "list", "ls":
		handleJobList(ctx, client, flags)
	case "get", "show":
		handleJobGet(ctx, client, subArgs, flags)
	case "cancel", "kill":
		handleJobCancel(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown job subcommand: %s\n", subcommand)
		printJobUsage()
		os.Exit(1)
	}
}

func printJobUsage() {
	fmt.Printf(`Job Management Commands

USAGE:
    tdcli jobs <subcommand> [options]
    tdcli job <subcommand> [options]

SUBCOMMANDS:
    list, ls               List jobs
    get, show <job_id>     Get job details
    cancel, kill <job_id>  Cancel a running job

OPTIONS:
    --status STATUS        Filter by job status
    --format FORMAT        Output format (json, table, csv)
    --verbose, -v          Verbose output

EXAMPLES:
    tdcli job list
    tdcli job list --status running
    tdcli job show 12345
    tdcli job cancel 12345

`)
}

func handleJobList(ctx context.Context, client *td.Client, flags Flags) {
	var opts *td.JobListOptions
	if flags.Status != "" {
		opts = &td.JobListOptions{Status: flags.Status}
	}

	jobsResp, err := client.Jobs.List(ctx, opts)
	handleError(err, "Failed to list jobs", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(jobsResp.Jobs)
	case "csv":
		printJobsCSV(jobsResp.Jobs)
	default:
		printJobsTable(jobsResp.Jobs)
	}
}

func handleJobGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Job ID required")
		fmt.Println("Usage: tdcli job get <job_id>")
		os.Exit(1)
	}

	jobID := args[0]
	job, err := client.Jobs.Get(ctx, jobID)
	handleError(err, "Failed to get job", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(job)
	default:
		printJobDetails(*job)
	}
}

func handleJobCancel(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Job ID required")
		fmt.Println("Usage: tdcli job cancel <job_id>")
		os.Exit(1)
	}

	jobID := args[0]

	// Confirm cancellation
	fmt.Printf("Are you sure you want to cancel job '%s'? (y/N): ", jobID)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Cancellation cancelled")
		return
	}

	err := client.Jobs.Kill(ctx, jobID)
	handleError(err, "Failed to cancel job", flags.Verbose)

	fmt.Printf("Job %s cancelled\n", jobID)
}

func printJobDetails(job td.Job) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PROPERTY\tVALUE")
	fmt.Fprintf(w, "Job ID\t%s\n", job.JobID)
	fmt.Fprintf(w, "Status\t%s\n", job.Status)
	fmt.Fprintf(w, "Type\t%s\n", job.Type)
	fmt.Fprintf(w, "Database\t%s\n", job.Database)
	fmt.Fprintf(w, "Created\t%s\n", formatTDTime(job.CreatedAt))
	if !job.StartAt.Time.IsZero() {
		fmt.Fprintf(w, "Started\t%s\n", formatTDTime(job.StartAt))
	}
	if !job.EndAt.Time.IsZero() {
		fmt.Fprintf(w, "Ended\t%s\n", formatTDTime(job.EndAt))
	}
	if job.CPUTime != nil {
		fmt.Fprintf(w, "CPU Time\t%ds\n", *job.CPUTime)
	}
	fmt.Fprintf(w, "Result Size\t%d\n", job.ResultSize)
	fmt.Fprintf(w, "Records\t%d\n", job.NumRecords)
	w.Flush()

	if job.Query.Value != "" {
		fmt.Printf("\nQuery:\n%s\n", job.Query.Value)
	}
	if job.Debug != nil && job.Debug.Stderr != "" {
		fmt.Printf("\nError Details:\n%s\n", job.Debug.Stderr)
	}
}

func printJobsTable(jobs []td.Job) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "JOB_ID\tSTATUS\tTYPE\tDATABASE\tCREATED\tDURATION")

	for _, job := range jobs {
		createdAt := formatTDTime(job.CreatedAt)

		var duration string
		if !job.StartAt.Time.IsZero() && !job.EndAt.Time.IsZero() {
			d := job.EndAt.Time.Sub(job.StartAt.Time)
			duration = fmt.Sprintf("%.1fs", d.Seconds())
		} else {
			duration = "-"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			job.JobID,
			job.Status,
			job.Type,
			job.Database,
			createdAt,
			duration,
		)
	}
	w.Flush()
}

func printJobsCSV(jobs []td.Job) {
	fmt.Println("job_id,status,type,database,created,duration_seconds")
	for _, job := range jobs {
		var duration string
		if !job.StartAt.Time.IsZero() && !job.EndAt.Time.IsZero() {
			d := job.EndAt.Time.Sub(job.StartAt.Time)
			duration = fmt.Sprintf("%.1f", d.Seconds())
		} else {
			duration = ""
		}

		fmt.Printf("%s,%s,%s,%s,%s,%s\n",
			job.JobID,
			job.Status,
			job.Type,
			job.Database,
			formatTDTime(job.CreatedAt),
			duration,
		)
	}
}
