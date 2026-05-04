package main

type TablesCmd struct {
	List   TablesListCmd   `kong:"cmd,aliases='ls',help='List tables in database'"`
	Get    TablesGetCmd    `kong:"cmd,aliases='show',help='Get table details'"`
	Create TablesCreateCmd `kong:"cmd,help='Create a table'"`
	Delete TablesDeleteCmd `kong:"cmd,aliases='rm',help='Delete a table'"`
	Swap   TablesSwapCmd   `kong:"cmd,help='Swap two tables'"`
	Rename TablesRenameCmd `kong:"cmd,aliases='mv',help='Rename a table'"`
}

type TablesListCmd struct {
	Database string `kong:"arg,help='Database name'"`
}

func (t *TablesListCmd) Run(ctx *CLIContext) error {
	return InstrumentedRun(ctx, "tables.list", []string{t.Database}, func(ctx *CLIContext) error {
		return runHandlerWithErrorCapture(func() {
			handleTableList(ctx.Context, ctx.Client, []string{t.Database}, ctx.GlobalFlags)
		})
	})
}

type TablesGetCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (t *TablesGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleTableGet(ctx.Context, ctx.Client, []string{t.Database, t.Table}, ctx.GlobalFlags)
	})
}

type TablesCreateCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (t *TablesCreateCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleTableCreate(ctx.Context, ctx.Client, []string{t.Database, t.Table}, ctx.GlobalFlags)
	})
}

type TablesDeleteCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (t *TablesDeleteCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleTableDelete(ctx.Context, ctx.Client, []string{t.Database, t.Table}, ctx.GlobalFlags)
	})
}

type TablesSwapCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table1   string `kong:"arg,help='First table name'"`
	Table2   string `kong:"arg,help='Second table name'"`
}

func (t *TablesSwapCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleTableSwap(ctx.Context, ctx.Client, []string{t.Database, t.Table1, t.Table2}, ctx.GlobalFlags)
	})
}

type TablesRenameCmd struct {
	Database string `kong:"arg,help='Database name'"`
	OldName  string `kong:"arg,help='Current table name'"`
	NewName  string `kong:"arg,help='New table name'"`
}

func (t *TablesRenameCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleTableRename(ctx.Context, ctx.Client, []string{t.Database, t.OldName, t.NewName}, ctx.GlobalFlags)
	})
}
