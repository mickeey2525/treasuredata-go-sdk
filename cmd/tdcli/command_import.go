package main

type ImportCmd struct {
	List     ImportListCmd     `kong:"cmd,aliases='ls',help='List bulk import sessions'"`
	Get      ImportGetCmd      `kong:"cmd,aliases='show',help='Get bulk import session details'"`
	Create   ImportCreateCmd   `kong:"cmd,help='Create a new bulk import session'"`
	Delete   ImportDeleteCmd   `kong:"cmd,aliases='rm',help='Delete a bulk import session'"`
	Upload   ImportUploadCmd   `kong:"cmd,help='Upload a part to session'"`
	Commit   ImportCommitCmd   `kong:"cmd,help='Commit a bulk import session'"`
	Perform  ImportPerformCmd  `kong:"cmd,help='Perform bulk import job'"`
	Freeze   ImportFreezeCmd   `kong:"cmd,help='Freeze a bulk import session'"`
	Unfreeze ImportUnfreezeCmd `kong:"cmd,help='Unfreeze a bulk import session'"`
	Parts    ImportPartsCmd    `kong:"cmd,help='List parts in a bulk import session'"`
}

type ImportListCmd struct{}

func (i *ImportListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.list", []string{}, func() {
		handleBulkImportList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type ImportGetCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.get", []string{i.Session}, func() {
		handleBulkImportGet(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportCreateCmd struct {
	Session  string `kong:"arg,help='Session name'"`
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (i *ImportCreateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.create", []string{i.Session, i.Database, i.Table}, func() {
		handleBulkImportCreate(ctx.Context, ctx.Client, []string{i.Session, i.Database, i.Table}, ctx.GlobalFlags)
	})
}

type ImportDeleteCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportDeleteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.delete", []string{i.Session}, func() {
		handleBulkImportDelete(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportUploadCmd struct {
	Session  string `kong:"arg,help='Session name'"`
	PartName string `kong:"arg,help='Part name'"`
	FilePath string `kong:"arg,help='File path'"`
}

func (i *ImportUploadCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.upload", []string{i.Session, i.PartName, i.FilePath}, func() {
		handleBulkImportUpload(ctx.Context, ctx.Client, []string{i.Session, i.PartName, i.FilePath}, ctx.GlobalFlags)
	})
}

type ImportCommitCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportCommitCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.commit", []string{i.Session}, func() {
		handleBulkImportCommit(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportPerformCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportPerformCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.perform", []string{i.Session}, func() {
		handleBulkImportPerform(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportFreezeCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportFreezeCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.freeze", []string{i.Session}, func() {
		handleBulkImportFreeze(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportUnfreezeCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportUnfreezeCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.unfreeze", []string{i.Session}, func() {
		handleBulkImportUnfreeze(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportPartsCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportPartsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "bulk-import.parts", []string{i.Session}, func() {
		handleBulkImportParts(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}
