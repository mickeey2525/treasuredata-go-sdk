package main

type JobsCmd struct {
	List   JobsListCmd   `kong:"cmd,aliases='ls',help='List jobs'"`
	Get    JobsGetCmd    `kong:"cmd,aliases='show',help='Get job details'"`
	Cancel JobsCancelCmd `kong:"cmd,aliases='kill',help='Cancel a running job'"`
}

type JobsListCmd struct {
	Status string `kong:"help='Filter by job status'"`
}

func (j *JobsListCmd) Run(ctx *CLIContext) error {
	return InstrumentedRun(ctx, "jobs.list", []string{}, func(ctx *CLIContext) error {
		return runHandlerWithErrorCapture(func() {
			ctx.GlobalFlags.Status = j.Status
			handleJobList(ctx.Context, ctx.Client, ctx.GlobalFlags)
		})
	})
}

type JobsGetCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (j *JobsGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleJobGet(ctx.Context, ctx.Client, []string{j.JobID}, ctx.GlobalFlags)
	})
}

type JobsCancelCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (j *JobsCancelCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleJobCancel(ctx.Context, ctx.Client, []string{j.JobID}, ctx.GlobalFlags)
	})
}
