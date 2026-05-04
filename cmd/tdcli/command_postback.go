package main

type PostbackCmd struct {
	Send PostbackSendCmd `kong:"cmd,help='Send event via postback'"`
}

type PostbackSendCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
	Data     string `kong:"arg,help='Event data as JSON string'"`
}

func (p *PostbackSendCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "postback.send", []string{p.Database, p.Table}, func() {
		handlePostbackSend(ctx.Context, ctx.Client, []string{p.Database, p.Table, p.Data}, ctx.GlobalFlags)
	})
}
