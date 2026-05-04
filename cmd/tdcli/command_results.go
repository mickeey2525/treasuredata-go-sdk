package main

type ResultsCmd struct {
	Get ResultsGetCmd `kong:"cmd,aliases='show',help='Get query results'"`
}

type ResultsGetCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
	Limit int    `kong:"help='Limit number of rows'"`
}

func (r *ResultsGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		ctx.GlobalFlags.Limit = r.Limit
		handleResultGet(ctx.Context, ctx.Client, []string{r.JobID}, ctx.GlobalFlags)
	})
}
