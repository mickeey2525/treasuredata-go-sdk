package main

type DatabasesCmd struct {
	List   DatabasesListCmd   `kong:"cmd,aliases='ls',help='List databases'"`
	Get    DatabasesGetCmd    `kong:"cmd,aliases='show',help='Get database details'"`
	Create DatabasesCreateCmd `kong:"cmd,help='Create a database'"`
	Delete DatabasesDeleteCmd `kong:"cmd,aliases='rm',help='Delete a database'"`
	Update DatabasesUpdateCmd `kong:"cmd,help='Update database properties'"`
}

type DatabasesListCmd struct{}

func (d *DatabasesListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "databases.list", []string{}, func() {
		handleDatabaseList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type DatabasesGetCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "databases.get", []string{d.Name}, func() {
		handleDatabaseGet(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	})
}

type DatabasesCreateCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesCreateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "databases.create", []string{d.Name}, func() {
		handleDatabaseCreate(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	})
}

type DatabasesDeleteCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesDeleteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "databases.delete", []string{d.Name}, func() {
		handleDatabaseDelete(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	})
}

type DatabasesUpdateCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesUpdateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "databases.update", []string{d.Name}, func() {
		handleDatabaseUpdate(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	})
}
