package main

type StreamCmd struct {
	Import StreamImportCmd `kong:"cmd,help='Import data via stream import API'"`
}

type StreamImportCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
	FilePath string `kong:"arg,help='Path to msgpack.gz file'"`
	UniqueID string `kong:"arg,optional,help='Unique ID for deduplication'"`
}

func (s *StreamImportCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "stream.import", []string{s.Database, s.Table, s.FilePath}, func() error {
		return handleStreamImport(ctx.Context, ctx.Client, []string{s.Database, s.Table, s.FilePath, s.UniqueID}, ctx.GlobalFlags)
	})
}
