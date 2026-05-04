package main

type PersonalizationCmd struct {
	Send PersonalizationSendCmd `kong:"cmd,help='Send personalization event'"`
}

type PersonalizationSendCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
	Data     string `kong:"arg,help='Event data as JSON string'"`
	Token    string `kong:"help='Personalization token (WP13n-Token)'"`
}

func (p *PersonalizationSendCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "personalization.send", []string{p.Database, p.Table}, func() {
		handlePersonalizationSend(ctx.Context, ctx.Client, []string{p.Database, p.Table, p.Data, p.Token}, ctx.GlobalFlags)
	})
}
