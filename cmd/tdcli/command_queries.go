package main

type QueriesCmd struct {
	Submit QuerySubmitCmd `kong:"cmd,aliases='run',help='Submit a query for execution'"`
	Status QueryStatusCmd `kong:"cmd,help='Check query execution status'"`
	Result QueryResultCmd `kong:"cmd,aliases='results',help='Get query results'"`
	List   QueryListCmd   `kong:"cmd,aliases='ls',help='List recent queries'"`
	Cancel QueryCancelCmd `kong:"cmd,help='Cancel a running query'"`
}

type QuerySubmitCmd struct {
	Query    string `kong:"arg,help='SQL query to execute'"`
	Database string `kong:"required,help='Database to run query against'"`
	Engine   string `kong:"help='Query engine: trino (default) or hive',default='trino',enum='trino,hive,presto'"`
	Priority int    `kong:"help='Query priority (0-2)',default=0"`
	Wait     bool   `kong:"help='Wait for query completion'"`
	Timeout  int    `kong:"help='Wait timeout in seconds',default=300"`
}

func (q *QuerySubmitCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "queries.submit", []string{q.Query}, func() error {
		ctx.GlobalFlags.Database = q.Database
		ctx.GlobalFlags.Priority = q.Priority
		ctx.GlobalFlags.Engine = q.Engine
		return handleQuerySubmit(ctx.Context, ctx.Client, []string{q.Query}, ctx.GlobalFlags)
	})
}

type QueryStatusCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (q *QueryStatusCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "queries.status", []string{q.JobID}, func() error {
		return handleQueryStatus(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
	})
}

type QueryResultCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
	Limit int    `kong:"help='Limit number of result rows'"`
}

func (q *QueryResultCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "queries.result", []string{q.JobID}, func() error {
		ctx.GlobalFlags.Limit = q.Limit
		return handleQueryResult(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
	})
}

type QueryListCmd struct {
	Status string `kong:"help='Filter by job status'"`
}

func (q *QueryListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "queries.list", []string{}, func() error {
		ctx.GlobalFlags.Status = q.Status
		return handleQueryList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type QueryCancelCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (q *QueryCancelCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "queries.cancel", []string{q.JobID}, func() error {
		return handleQueryCancel(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
	})
}
