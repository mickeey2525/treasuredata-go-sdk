package main

type LLMCmd struct {
	Actions      LLMActionsCmd      `kong:"cmd,aliases='action',help='LLM action management'"`
	Integrations LLMIntegrationsCmd `kong:"cmd,aliases='integration',help='LLM integration management'"`
	Prompts      LLMPromptsCmd      `kong:"cmd,aliases='prompt',help='LLM prompt management'"`
	Projects     LLMProjectsCmd     `kong:"cmd,aliases='project',help='LLM project management'"`
}

type LLMActionsCmd struct {
	List    LLMActionsListCmd    `kong:"cmd,aliases='ls',help='List actions'"`
	Get     LLMActionsGetCmd     `kong:"cmd,aliases='show',help='Get action details'"`
	Execute LLMActionsExecuteCmd `kong:"cmd,help='Execute an action'"`
}

type LLMActionsListCmd struct{}

func (l *LLMActionsListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "llm.actions.list", []string{}, func() {
		handleLLMActionList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type LLMActionsGetCmd struct {
	ActionID string `kong:"arg,help='Action ID'"`
}

func (l *LLMActionsGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "llm.actions.get", []string{l.ActionID}, func() {
		handleLLMActionGet(ctx.Context, ctx.Client, []string{l.ActionID}, ctx.GlobalFlags)
	})
}

type LLMActionsExecuteCmd struct {
	ActionID string `kong:"arg,help='Action ID'"`
	Input    string `kong:"arg,help='Input data as JSON string'"`
}

func (l *LLMActionsExecuteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "llm.actions.execute", []string{l.ActionID}, func() {
		handleLLMActionExecute(ctx.Context, ctx.Client, []string{l.ActionID, l.Input}, ctx.GlobalFlags)
	})
}

type LLMIntegrationsCmd struct {
	List LLMIntegrationsListCmd `kong:"cmd,aliases='ls',help='List integrations'"`
}

type LLMIntegrationsListCmd struct{}

func (l *LLMIntegrationsListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "llm.integrations.list", []string{}, func() {
		handleLLMIntegrationList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type LLMPromptsCmd struct {
	List LLMPromptsListCmd `kong:"cmd,aliases='ls',help='List prompts'"`
}

type LLMPromptsListCmd struct{}

func (l *LLMPromptsListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "llm.prompts.list", []string{}, func() {
		handleLLMPromptList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type LLMProjectsCmd struct {
	List LLMProjectsListCmd `kong:"cmd,aliases='ls',help='List projects'"`
	Get  LLMProjectsGetCmd  `kong:"cmd,aliases='show',help='Get project details'"`
}

type LLMProjectsListCmd struct{}

func (l *LLMProjectsListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "llm.projects.list", []string{}, func() {
		handleLLMProjectList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type LLMProjectsGetCmd struct {
	ProjectID string `kong:"arg,help='Project ID'"`
}

func (l *LLMProjectsGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "llm.projects.get", []string{l.ProjectID}, func() {
		handleLLMProjectGet(ctx.Context, ctx.Client, []string{l.ProjectID}, ctx.GlobalFlags)
	})
}
