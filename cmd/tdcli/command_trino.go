package main

type TrinoCmd struct {
	Query       TrinoQueryCmd       `kong:"cmd,aliases='q',help='Execute a Trino query'"`
	Interactive TrinoInteractiveCmd `kong:"cmd,aliases='i,repl',help='Start interactive Trino session'"`
	Test        TrinoTestCmd        `kong:"cmd,help='Test Trino connection'"`
	Describe    TrinoDescribeCmd    `kong:"cmd,aliases='desc',help='Describe a table'"`
	Show        TrinoShowCmd        `kong:"cmd,help='Show schemas, tables, or columns'"`
	Explain     TrinoExplainCmd     `kong:"cmd,help='Explain query execution plan'"`
	Version     TrinoVersionCmd     `kong:"cmd,help='Show Trino version'"`
}

type TrinoQueryCmd struct {
	Query    string `kong:"arg,help='SQL query to execute'"`
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
	Limit    int    `kong:"help='Limit number of result rows'"`
	PageSize int    `kong:"help='Page size for pagination (0 = no pagination)',default='0'"`
}

func (t *TrinoQueryCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	ctx.GlobalFlags.Limit = t.Limit
	return runInstrumented(ctx, "trino.query", []string{t.Query}, func() error {
		if t.PageSize > 0 {
			return handleTrinoQueryWithPagination(ctx.Context, ctx.Client, []string{t.Query}, ctx.GlobalFlags, t.PageSize)
		}
		return handleTrinoQuery(ctx.Context, ctx.Client, []string{t.Query}, ctx.GlobalFlags)
	})
}

type TrinoInteractiveCmd struct {
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoInteractiveCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	return runInstrumented(ctx, "trino.interactive", []string{}, func() error {
		return handleTrinoInteractive(ctx.Context, ctx.Client, []string{}, ctx.GlobalFlags)
	})
}

type TrinoTestCmd struct {
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoTestCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	return runInstrumented(ctx, "trino.test", []string{}, func() error {
		return handleTrinoTest(ctx.Context, ctx.Client, []string{}, ctx.GlobalFlags)
	})
}

type TrinoDescribeCmd struct {
	Table    string `kong:"arg,help='Table name to describe'"`
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoDescribeCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	return runInstrumented(ctx, "trino.describe", []string{t.Table}, func() error {
		return handleTrinoDescribe(ctx.Context, ctx.Client, []string{t.Table}, ctx.GlobalFlags)
	})
}

type TrinoShowCmd struct {
	Type     string `kong:"arg,help='What to show: schemas, tables, columns',enum='schemas,tables,columns'"`
	Table    string `kong:"help='Table name (required for columns)'"`
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoShowCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	args := []string{t.Type}
	if t.Table != "" {
		args = append(args, t.Table)
	}
	return runInstrumented(ctx, "trino.show", args, func() error {
		return handleTrinoShow(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type TrinoExplainCmd struct {
	Query    string `kong:"arg,help='Query to explain'"`
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoExplainCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	return runInstrumented(ctx, "trino.explain", []string{t.Query}, func() error {
		return handleTrinoExplain(ctx.Context, ctx.Client, []string{t.Query}, ctx.GlobalFlags)
	})
}

type TrinoVersionCmd struct {
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoVersionCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	return runInstrumented(ctx, "trino.version", []string{}, func() error {
		return handleTrinoVersion(ctx.Context, ctx.Client, []string{}, ctx.GlobalFlags)
	})
}
