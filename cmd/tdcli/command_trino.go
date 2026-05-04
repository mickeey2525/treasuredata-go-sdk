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
	if t.PageSize > 0 {
		handleTrinoQueryWithPagination(ctx.Context, ctx.Client, []string{t.Query}, ctx.GlobalFlags, t.PageSize)
	} else {
		handleTrinoQuery(ctx.Context, ctx.Client, []string{t.Query}, ctx.GlobalFlags)
	}
	return nil
}

type TrinoInteractiveCmd struct {
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoInteractiveCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	handleTrinoInteractive(ctx.Context, ctx.Client, []string{}, ctx.GlobalFlags)
	return nil
}

type TrinoTestCmd struct {
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoTestCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	handleTrinoTest(ctx.Context, ctx.Client, []string{}, ctx.GlobalFlags)
	return nil
}

type TrinoDescribeCmd struct {
	Table    string `kong:"arg,help='Table name to describe'"`
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoDescribeCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	handleTrinoDescribe(ctx.Context, ctx.Client, []string{t.Table}, ctx.GlobalFlags)
	return nil
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
	handleTrinoShow(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type TrinoExplainCmd struct {
	Query    string `kong:"arg,help='Query to explain'"`
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoExplainCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	handleTrinoExplain(ctx.Context, ctx.Client, []string{t.Query}, ctx.GlobalFlags)
	return nil
}

type TrinoVersionCmd struct {
	Database string `kong:"help='Database/schema to use',default='sample_datasets'"`
}

func (t *TrinoVersionCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Database = t.Database
	handleTrinoVersion(ctx.Context, ctx.Client, []string{}, ctx.GlobalFlags)
	return nil
}
