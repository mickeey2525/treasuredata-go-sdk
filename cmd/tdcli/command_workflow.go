package main

import (
	"fmt"

	"github.com/mickeey2525/treasuredata-go-sdk/cmd/tdcli/workflow"
)

type WorkflowCmd struct {
	List     WorkflowListCmd     `kong:"cmd,aliases='ls',help='List workflows'"`
	Get      WorkflowGetCmd      `kong:"cmd,aliases='show',help='Get workflow details'"`
	Create   WorkflowCreateCmd   `kong:"cmd,help='Create a new workflow'"`
	Update   WorkflowUpdateCmd   `kong:"cmd,help='Update workflow'"`
	Delete   WorkflowDeleteCmd   `kong:"cmd,aliases='rm',help='Delete workflow'"`
	Start    WorkflowStartCmd    `kong:"cmd,aliases='run',help='Start workflow execution'"`
	Init     WorkflowInitCmd     `kong:"cmd,help='Create a sample workflow project'"`
	Attempts WorkflowAttemptsCmd `kong:"cmd,aliases='attempt',help='Workflow attempt management'"`
	Schedule WorkflowScheduleCmd `kong:"cmd,help='Workflow schedule management'"`
	Tasks    WorkflowTasksCmd    `kong:"cmd,aliases='task',help='Workflow task management'"`
	Logs     WorkflowLogsCmd     `kong:"cmd,aliases='log',help='Workflow log management'"`
	Projects WorkflowProjectsCmd `kong:"cmd,aliases='project,proj',help='Workflow project management'"`
}

type WorkflowListCmd struct{}

func (w *WorkflowListCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.list", []string{}, func() {
		workflow.HandleWorkflowList(ctx.Context, ctx.Client, flags)
	})
}

type WorkflowGetCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowGetCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.get", []string{fmt.Sprintf("%d", w.WorkflowID)}, func() {
		workflow.HandleWorkflowGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	})
}

type WorkflowCreateCmd struct {
	Name    string `kong:"arg,help='Workflow name'"`
	Project string `kong:"arg,help='Project name'"`
	Config  string `kong:"arg,help='Workflow configuration (YAML)'"`
}

func (w *WorkflowCreateCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.create", []string{w.Name, w.Project}, func() {
		workflow.HandleWorkflowCreate(ctx.Context, ctx.Client, []string{w.Name, w.Project, w.Config}, flags)
	})
}

type WorkflowUpdateCmd struct {
	WorkflowID int      `kong:"arg,help='Workflow ID'"`
	Updates    []string `kong:"arg,help='Updates (key=value)'"`
}

func (w *WorkflowUpdateCmd) Run(ctx *CLIContext) error {
	args := append([]string{fmt.Sprintf("%d", w.WorkflowID)}, w.Updates...)
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.update", args, func() {
		workflow.HandleWorkflowUpdate(ctx.Context, ctx.Client, args, flags)
	})
}

type WorkflowDeleteCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowDeleteCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.delete", []string{fmt.Sprintf("%d", w.WorkflowID)}, func() {
		workflow.HandleWorkflowDelete(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	})
}

type WorkflowStartCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	Params     string `kong:"help='Parameters (JSON)'"`
}

func (w *WorkflowStartCmd) Run(ctx *CLIContext) error {
	args := []string{fmt.Sprintf("%d", w.WorkflowID)}
	if w.Params != "" {
		args = append(args, w.Params)
	}
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.start", []string{fmt.Sprintf("%d", w.WorkflowID)}, func() {
		workflow.HandleWorkflowStart(ctx.Context, ctx.Client, args, flags)
	})
}

type WorkflowAttemptsCmd struct {
	List  WorkflowAttemptsListCmd  `kong:"cmd,aliases='ls',help='List workflow attempts'"`
	Get   WorkflowAttemptsGetCmd   `kong:"cmd,aliases='show',help='Get attempt details'"`
	Kill  WorkflowAttemptsKillCmd  `kong:"cmd,help='Kill running attempt'"`
	Retry WorkflowAttemptsRetryCmd `kong:"cmd,help='Retry failed attempt'"`
}

type WorkflowAttemptsListCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowAttemptsListCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.attempts.list", []string{fmt.Sprintf("%d", w.WorkflowID)}, func() {
		workflow.HandleWorkflowAttemptList(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	})
}

type WorkflowAttemptsGetCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
	AttemptID  int `kong:"arg,help='Attempt ID'"`
}

func (w *WorkflowAttemptsGetCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.attempts.get", []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, func() {
		workflow.HandleWorkflowAttemptGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, flags)
	})
}

type WorkflowAttemptsKillCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
	AttemptID  int `kong:"arg,help='Attempt ID'"`
}

func (w *WorkflowAttemptsKillCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.attempts.kill", []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, func() {
		workflow.HandleWorkflowAttemptKill(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, flags)
	})
}

type WorkflowAttemptsRetryCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	AttemptID  int    `kong:"arg,help='Attempt ID'"`
	Params     string `kong:"help='Parameters (JSON)'"`
}

func (w *WorkflowAttemptsRetryCmd) Run(ctx *CLIContext) error {
	args := []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}
	if w.Params != "" {
		args = append(args, w.Params)
	}
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.attempts.retry", []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, func() {
		workflow.HandleWorkflowAttemptRetry(ctx.Context, ctx.Client, args, flags)
	})
}

type WorkflowScheduleCmd struct {
	Get     WorkflowScheduleGetCmd     `kong:"cmd,aliases='show',help='Get workflow schedule'"`
	Enable  WorkflowScheduleEnableCmd  `kong:"cmd,help='Enable workflow schedule'"`
	Disable WorkflowScheduleDisableCmd `kong:"cmd,help='Disable workflow schedule'"`
	Update  WorkflowScheduleUpdateCmd  `kong:"cmd,help='Update workflow schedule'"`
}

type WorkflowScheduleGetCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowScheduleGetCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.schedule.get", []string{fmt.Sprintf("%d", w.WorkflowID)}, func() {
		workflow.HandleWorkflowScheduleGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	})
}

type WorkflowScheduleEnableCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowScheduleEnableCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.schedule.enable", []string{fmt.Sprintf("%d", w.WorkflowID)}, func() {
		workflow.HandleWorkflowScheduleEnable(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	})
}

type WorkflowScheduleDisableCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowScheduleDisableCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.schedule.disable", []string{fmt.Sprintf("%d", w.WorkflowID)}, func() {
		workflow.HandleWorkflowScheduleDisable(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	})
}

type WorkflowScheduleUpdateCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	Cron       string `kong:"arg,help='Cron expression'"`
	Timezone   string `kong:"arg,help='Timezone'"`
	Delay      int    `kong:"arg,help='Delay in seconds'"`
}

func (w *WorkflowScheduleUpdateCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.schedule.update", []string{fmt.Sprintf("%d", w.WorkflowID)}, func() {
		workflow.HandleWorkflowScheduleUpdate(ctx.Context, ctx.Client, []string{
			fmt.Sprintf("%d", w.WorkflowID), w.Cron, w.Timezone, fmt.Sprintf("%d", w.Delay),
		}, flags)
	})
}

type WorkflowTasksCmd struct {
	List WorkflowTasksListCmd `kong:"cmd,aliases='ls',help='List workflow tasks'"`
	Get  WorkflowTasksGetCmd  `kong:"cmd,aliases='show',help='Get task details'"`
}

type WorkflowTasksListCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
	AttemptID  int `kong:"arg,help='Attempt ID'"`
}

func (w *WorkflowTasksListCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.tasks.list", []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, func() {
		workflow.HandleWorkflowTaskList(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, flags)
	})
}

type WorkflowTasksGetCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	AttemptID  int    `kong:"arg,help='Attempt ID'"`
	TaskID     string `kong:"arg,help='Task ID'"`
}

func (w *WorkflowTasksGetCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.tasks.get", []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID), w.TaskID}, func() {
		workflow.HandleWorkflowTaskGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID), w.TaskID}, flags)
	})
}

type WorkflowLogsCmd struct {
	Attempt WorkflowLogsAttemptCmd `kong:"cmd,help='Get attempt log'"`
	Task    WorkflowLogsTaskCmd    `kong:"cmd,help='Get task log'"`
}

type WorkflowLogsAttemptCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
	AttemptID  int `kong:"arg,help='Attempt ID'"`
}

func (w *WorkflowLogsAttemptCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.logs.attempt", []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, func() {
		workflow.HandleWorkflowAttemptLog(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, flags)
	})
}

type WorkflowLogsTaskCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	AttemptID  int    `kong:"arg,help='Attempt ID'"`
	TaskID     string `kong:"arg,help='Task ID'"`
}

func (w *WorkflowLogsTaskCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.logs.task", []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID), w.TaskID}, func() {
		workflow.HandleWorkflowTaskLog(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID), w.TaskID}, flags)
	})
}

type WorkflowInitCmd struct {
	ProjectName string `kong:"arg,help='Name of the new workflow project'"`
}

func (w *WorkflowInitCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.init", []string{w.ProjectName}, func() {
		workflow.HandleWorkflowInit(ctx.Context, []string{w.ProjectName}, flags)
	})
}

type WorkflowProjectsCmd struct {
	List      WorkflowProjectsListCmd      `kong:"cmd,aliases='ls',help='List workflow projects'"`
	Get       WorkflowProjectsGetCmd       `kong:"cmd,aliases='show',help='Get project details'"`
	Create    WorkflowProjectsCreateCmd    `kong:"cmd,help='Create a new project'"`
	Push      WorkflowProjectsPushCmd      `kong:"cmd,help='Push project from directory (alias for create)'"`
	Download  WorkflowProjectsDownloadCmd  `kong:"cmd,help='Download project archive and extract to directory'"`
	Workflows WorkflowProjectsWorkflowsCmd `kong:"cmd,aliases='wf',help='List workflows in project'"`
	Secrets   WorkflowProjectsSecretsCmd   `kong:"cmd,aliases='secret',help='Project secrets management'"`
	Hooks     WorkflowProjectsHooksCmd     `kong:"cmd,aliases='hook',help='Workflow hooks management'"`
}

type WorkflowProjectsListCmd struct{}

func (w *WorkflowProjectsListCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.list", []string{}, func() {
		workflow.HandleWorkflowProjectList(ctx.Context, ctx.Client, flags)
	})
}

type WorkflowProjectsGetCmd struct {
	ProjectID string `kong:"arg,help='Project ID or name'"`
}

func (w *WorkflowProjectsGetCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.get", []string{w.ProjectID}, func() {
		workflow.HandleWorkflowProjectGet(ctx.Context, ctx.Client, []string{w.ProjectID}, flags)
	})
}

type WorkflowProjectsCreateCmd struct {
	Name string `kong:"arg,help='Project name'"`
	Path string `kong:"arg,help='Directory path or archive file path'"`
}

func (w *WorkflowProjectsCreateCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.create", []string{w.Name}, func() {
		workflow.HandleWorkflowProjectCreate(ctx.Context, ctx.Client, []string{w.Name, w.Path}, flags)
	})
}

type WorkflowProjectsPushCmd struct {
	Name string `kong:"arg,help='Project name'"`
	Path string `kong:"arg,help='Directory path or archive file path'"`
}

func (w *WorkflowProjectsPushCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.push", []string{w.Name}, func() {
		workflow.HandleWorkflowProjectCreate(ctx.Context, ctx.Client, []string{w.Name, w.Path}, flags)
	})
}

type WorkflowProjectsDownloadCmd struct {
	ProjectIdentifier string `kong:"arg,help='Project ID or name'"`
	OutputDir         string `kong:"optional,help='Output directory (defaults to project name)'"`
	Revision          string `kong:"help='Specific revision to download'"`
}

func (w *WorkflowProjectsDownloadCmd) Run(ctx *CLIContext) error {
	args := []string{w.ProjectIdentifier}
	if w.OutputDir != "" {
		args = append(args, w.OutputDir)
	}
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.download", []string{w.ProjectIdentifier}, func() {
		workflow.HandleWorkflowProjectDownload(ctx.Context, ctx.Client, args, flags)
	})
}

type WorkflowProjectsWorkflowsCmd struct {
	ProjectID int `kong:"arg,help='Project ID'"`
}

func (w *WorkflowProjectsWorkflowsCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.workflows", []string{fmt.Sprintf("%d", w.ProjectID)}, func() {
		workflow.HandleWorkflowProjectWorkflows(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID)}, flags)
	})
}

type WorkflowProjectsSecretsCmd struct {
	List   WorkflowProjectsSecretsListCmd   `kong:"cmd,aliases='ls',help='List project secrets'"`
	Set    WorkflowProjectsSecretsSetCmd    `kong:"cmd,help='Set project secret'"`
	Delete WorkflowProjectsSecretsDeleteCmd `kong:"cmd,aliases='rm',help='Delete project secret'"`
}

type WorkflowProjectsSecretsListCmd struct {
	ProjectID int `kong:"arg,help='Project ID'"`
}

func (w *WorkflowProjectsSecretsListCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.secrets.list", []string{fmt.Sprintf("%d", w.ProjectID)}, func() {
		workflow.HandleWorkflowProjectSecretsList(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID)}, flags)
	})
}

type WorkflowProjectsSecretsSetCmd struct {
	ProjectID int    `kong:"arg,help='Project ID'"`
	Key       string `kong:"arg,help='Secret key'"`
	Value     string `kong:"arg,help='Secret value'"`
}

func (w *WorkflowProjectsSecretsSetCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.secrets.set", []string{fmt.Sprintf("%d", w.ProjectID), w.Key}, func() {
		workflow.HandleWorkflowProjectSecretsSet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID), w.Key, w.Value}, flags)
	})
}

type WorkflowProjectsSecretsDeleteCmd struct {
	ProjectID int    `kong:"arg,help='Project ID'"`
	Key       string `kong:"arg,help='Secret key'"`
}

func (w *WorkflowProjectsSecretsDeleteCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.secrets.delete", []string{fmt.Sprintf("%d", w.ProjectID), w.Key}, func() {
		workflow.HandleWorkflowProjectSecretsDelete(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID), w.Key}, flags)
	})
}

type WorkflowProjectsHooksCmd struct {
	Show   WorkflowProjectsHooksShowCmd   `kong:"cmd,aliases='ls,list',help='Show hooks configuration'"`
	Init   WorkflowProjectsHooksInitCmd   `kong:"cmd,help='Initialize hooks configuration file'"`
	Add    WorkflowProjectsHooksAddCmd    `kong:"cmd,help='Add a new hook'"`
	Remove WorkflowProjectsHooksRemoveCmd `kong:"cmd,aliases='rm',help='Remove a hook'"`
	Test   WorkflowProjectsHooksTestCmd   `kong:"cmd,help='Validate hooks configuration'"`
}

type WorkflowProjectsHooksShowCmd struct {
	Path string `kong:"help='Project directory path',default='.'"`
}

func (w *WorkflowProjectsHooksShowCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.hooks.show", []string{w.Path}, func() {
		workflow.HandleWorkflowHooksShow(ctx.Context, ctx.Client, []string{w.Path}, flags)
	})
}

type WorkflowProjectsHooksInitCmd struct {
	Path string `kong:"help='Project directory path',default='.'"`
}

func (w *WorkflowProjectsHooksInitCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.hooks.init", []string{w.Path}, func() {
		workflow.HandleWorkflowHooksInit(ctx.Context, ctx.Client, []string{w.Path}, flags)
	})
}

type WorkflowProjectsHooksAddCmd struct {
	Path        string   `kong:"help='Project directory path',default='.'"`
	Name        string   `kong:"arg,help='Hook name'"`
	Command     []string `kong:"arg,help='Hook command'"`
	Timeout     int      `kong:"help='Hook timeout in seconds',default='60'"`
	FailOnError bool     `kong:"help='Fail upload if hook fails',default='true'"`
	WorkingDir  string   `kong:"help='Working directory for hook execution'"`
}

func (w *WorkflowProjectsHooksAddCmd) Run(ctx *CLIContext) error {
	args := []string{w.Path, w.Name, fmt.Sprintf("%d", w.Timeout), fmt.Sprintf("%t", w.FailOnError), w.WorkingDir}
	args = append(args, w.Command...)
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.hooks.add", []string{w.Path, w.Name}, func() {
		workflow.HandleWorkflowHooksAdd(ctx.Context, ctx.Client, args, flags)
	})
}

type WorkflowProjectsHooksRemoveCmd struct {
	Path string `kong:"help='Project directory path',default='.'"`
	Name string `kong:"arg,help='Hook name to remove'"`
}

func (w *WorkflowProjectsHooksRemoveCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.hooks.remove", []string{w.Path, w.Name}, func() {
		workflow.HandleWorkflowHooksRemove(ctx.Context, ctx.Client, []string{w.Path, w.Name}, flags)
	})
}

type WorkflowProjectsHooksTestCmd struct {
	Path string `kong:"help='Project directory path',default='.'"`
}

func (w *WorkflowProjectsHooksTestCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	return runInstrumented(ctx, "workflow.projects.hooks.test", []string{w.Path}, func() {
		workflow.HandleWorkflowHooksValidate(ctx.Context, ctx.Client, []string{w.Path}, flags)
	})
}
