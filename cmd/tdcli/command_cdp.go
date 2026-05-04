package main

import "fmt"

type CDPCmd struct {
	Segments            CDPSegmentsCmd            `kong:"cmd,aliases='segment',help='CDP segment management'"`
	Audiences           CDPAudiencesCmd           `kong:"cmd,aliases='audience',help='CDP audience management'"`
	Activations         CDPActivationsCmd         `kong:"cmd,aliases='activation',help='CDP activation management'"`
	Folders             CDPFoldersCmd             `kong:"cmd,aliases='folder',help='CDP folder management'"`
	Tokens              CDPTokensCmd              `kong:"cmd,aliases='token',help='CDP token management'"`
	Journeys            CDPJourneysCmd            `kong:"cmd,aliases='journey',help='CDP journey management'"`
	ActivationTemplates CDPActivationTemplatesCmd `kong:"cmd,aliases='activation-template',help='CDP activation template management'"`
}

// --- Segments ---

type CDPSegmentsCmd struct {
	Create      CDPSegmentsCreateCmd      `kong:"cmd,help='Create a new segment'"`
	List        CDPSegmentsListCmd        `kong:"cmd,aliases='ls',help='List segments'"`
	Get         CDPSegmentsGetCmd         `kong:"cmd,aliases='show',help='Get segment details'"`
	Update      CDPSegmentsUpdateCmd      `kong:"cmd,help='Update segment'"`
	Delete      CDPSegmentsDeleteCmd      `kong:"cmd,aliases='rm',help='Delete segment'"`
	Folders     CDPSegmentsFoldersCmd     `kong:"cmd,help='Get segments in folder'"`
	Query       CDPSegmentsQueryCmd       `kong:"cmd,help='Execute segment query'"`
	NewQuery    CDPSegmentsNewQueryCmd    `kong:"cmd,aliases='new-query',help='Create new segment query'"`
	QueryStatus CDPSegmentsQueryStatusCmd `kong:"cmd,aliases='query-status',help='Get segment query status'"`
	KillQuery   CDPSegmentsKillQueryCmd   `kong:"cmd,aliases='kill-query',help='Kill segment query'"`
	Customers   CDPSegmentsCustomersCmd   `kong:"cmd,help='Get segment customers'"`
	Statistics  CDPSegmentsStatisticsCmd  `kong:"cmd,aliases='stats',help='Get segment statistics'"`
}

type CDPSegmentsCreateCmd struct {
	AudienceID  string `kong:"arg,help='Audience ID'"`
	Name        string `kong:"arg,help='Segment name'"`
	Description string `kong:"arg,help='Segment description'"`
	Query       string `kong:"arg,help='Segment query'"`
}

func (c *CDPSegmentsCreateCmd) Run(ctx *CLIContext) error {
	args := []string{c.AudienceID, c.Name}
	return runInstrumented(ctx, "cdp.segments.create", args, func() {
		handleCDPSegmentCreate(ctx.Context, ctx.Client, []string{c.AudienceID, c.Name, c.Description, c.Query}, ctx.GlobalFlags)
	})
}

type CDPSegmentsListCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPSegmentsListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.list", []string{c.AudienceID}, func() {
		handleCDPSegmentList(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPSegmentsGetCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
}

func (c *CDPSegmentsGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.get", []string{c.AudienceID, c.SegmentID}, func() {
		handleCDPSegmentGet(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID}, ctx.GlobalFlags)
	})
}

type CDPSegmentsUpdateCmd struct {
	AudienceID string   `kong:"arg,help='Audience ID'"`
	SegmentID  string   `kong:"arg,help='Segment ID'"`
	Updates    []string `kong:"arg,help='Updates (key=value)'"`
}

func (c *CDPSegmentsUpdateCmd) Run(ctx *CLIContext) error {
	args := append([]string{c.AudienceID, c.SegmentID}, c.Updates...)
	return runInstrumented(ctx, "cdp.segments.update", args, func() {
		handleCDPSegmentUpdate(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPSegmentsDeleteCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
}

func (c *CDPSegmentsDeleteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.delete", []string{c.AudienceID, c.SegmentID}, func() {
		handleCDPSegmentDelete(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID}, ctx.GlobalFlags)
	})
}

type CDPSegmentsFoldersCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	FolderID   string `kong:"arg,help='Folder ID'"`
}

func (c *CDPSegmentsFoldersCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.folders", []string{c.AudienceID, c.FolderID}, func() {
		handleCDPSegmentFolders(ctx.Context, ctx.Client, []string{c.AudienceID, c.FolderID}, ctx.GlobalFlags)
	})
}

type CDPSegmentsQueryCmd struct {
	AudienceID   string `kong:"arg,help='Audience ID'"`
	SegmentRules string `kong:"arg,help='Segment rules JSON'"`
}

func (c *CDPSegmentsQueryCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.query", []string{c.AudienceID}, func() {
		handleCDPSegmentQuery(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentRules}, ctx.GlobalFlags)
	})
}

type CDPSegmentsNewQueryCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
	Query      string `kong:"arg,help='Query text'"`
}

func (c *CDPSegmentsNewQueryCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.new_query", []string{c.AudienceID, c.SegmentID}, func() {
		handleCDPSegmentNewQuery(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.Query}, ctx.GlobalFlags)
	})
}

type CDPSegmentsQueryStatusCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
	QueryID    string `kong:"arg,help='Query ID'"`
}

func (c *CDPSegmentsQueryStatusCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.query_status", []string{c.AudienceID, c.SegmentID, c.QueryID}, func() {
		handleCDPSegmentQueryStatus(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.QueryID}, ctx.GlobalFlags)
	})
}

type CDPSegmentsKillQueryCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
	QueryID    string `kong:"arg,help='Query ID'"`
}

func (c *CDPSegmentsKillQueryCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.kill_query", []string{c.AudienceID, c.SegmentID, c.QueryID}, func() {
		handleCDPSegmentKillQuery(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.QueryID}, ctx.GlobalFlags)
	})
}

type CDPSegmentsCustomersCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
	Limit      int    `kong:"help='Limit number of results',default='100'"`
	Offset     int    `kong:"help='Offset for pagination',default='0'"`
	Fields     string `kong:"help='Comma-separated list of fields to include'"`
}

func (c *CDPSegmentsCustomersCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.customers", []string{c.AudienceID, c.SegmentID}, func() {
		handleCDPSegmentCustomers(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID}, ctx.GlobalFlags)
	})
}

type CDPSegmentsStatisticsCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
}

func (c *CDPSegmentsStatisticsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.segments.statistics", []string{c.AudienceID, c.SegmentID}, func() {
		handleCDPSegmentStatistics(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID}, ctx.GlobalFlags)
	})
}

// --- Audiences ---

type CDPAudiencesCmd struct {
	Create          CDPAudiencesCreateCmd          `kong:"cmd,help='Create a new audience'"`
	List            CDPAudiencesListCmd            `kong:"cmd,aliases='ls',help='List audiences'"`
	Get             CDPAudiencesGetCmd             `kong:"cmd,aliases='show',help='Get audience details'"`
	Delete          CDPAudiencesDeleteCmd          `kong:"cmd,aliases='rm',help='Delete audience'"`
	Behaviors       CDPAudiencesBehaviorsCmd       `kong:"cmd,help='Get audience behaviors'"`
	Run             CDPAudiencesRunCmd             `kong:"cmd,help='Run audience execution'"`
	Executions      CDPAudiencesExecutionsCmd      `kong:"cmd,help='Get audience executions history'"`
	Statistics      CDPAudiencesStatisticsCmd      `kong:"cmd,aliases='stats',help='Get audience statistics'"`
	SampleValues    CDPAudiencesSampleValuesCmd    `kong:"cmd,aliases='samples',help='Get audience sample values'"`
	BehaviorSamples CDPAudiencesBehaviorSamplesCmd `kong:"cmd,help='Get behavior sample values'"`
}

type CDPAudiencesCreateCmd struct {
	Name        string `kong:"arg,help='Audience name'"`
	Description string `kong:"arg,help='Audience description'"`
	SegmentIDs  string `kong:"arg,help='Segment IDs (comma-separated)'"`
}

func (c *CDPAudiencesCreateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.create", []string{c.Name}, func() {
		handleCDPAudienceCreate(ctx.Context, ctx.Client, []string{c.Name, c.Description, c.SegmentIDs}, ctx.GlobalFlags)
	})
}

type CDPAudiencesListCmd struct{}

func (c *CDPAudiencesListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.list", []string{}, func() {
		handleCDPAudienceList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type CDPAudiencesGetCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.get", []string{c.AudienceID}, func() {
		handleCDPAudienceGet(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPAudiencesDeleteCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesDeleteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.delete", []string{c.AudienceID}, func() {
		handleCDPAudienceDelete(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPAudiencesBehaviorsCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesBehaviorsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.behaviors", []string{c.AudienceID}, func() {
		handleCDPAudienceBehaviors(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPAudiencesRunCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesRunCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.run", []string{c.AudienceID}, func() {
		handleCDPAudienceRun(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPAudiencesExecutionsCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesExecutionsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.executions", []string{c.AudienceID}, func() {
		handleCDPAudienceExecutions(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPAudiencesStatisticsCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesStatisticsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.statistics", []string{c.AudienceID}, func() {
		handleCDPAudienceStatistics(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPAudiencesSampleValuesCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	Column     string `kong:"arg,help='Column name'"`
}

func (c *CDPAudiencesSampleValuesCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.sample_values", []string{c.AudienceID}, func() {
		handleCDPAudienceSampleValues(ctx.Context, ctx.Client, []string{c.AudienceID, c.Column}, ctx.GlobalFlags)
	})
}

type CDPAudiencesBehaviorSamplesCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	BehaviorID string `kong:"arg,help='Behavior ID'"`
	Column     string `kong:"arg,help='Column name'"`
}

func (c *CDPAudiencesBehaviorSamplesCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.audiences.behavior_samples", []string{c.AudienceID, c.BehaviorID}, func() {
		handleCDPAudienceBehaviorSamples(ctx.Context, ctx.Client, []string{c.AudienceID, c.BehaviorID, c.Column}, ctx.GlobalFlags)
	})
}

// --- Activations ---

type CDPActivationsCmd struct {
	Create              CDPActivationsCreateCmd              `kong:"cmd,help='Create activation'"`
	CreateWithStruct    CDPActivationsCreateWithStructCmd    `kong:"cmd,help='Create activation with struct'"`
	List                CDPActivationsListCmd                `kong:"cmd,aliases='ls',help='List activations'"`
	Get                 CDPActivationsGetCmd                 `kong:"cmd,aliases='show',help='Get activation details'"`
	Update              CDPActivationsUpdateCmd              `kong:"cmd,help='Update activation'"`
	UpdateStatus        CDPActivationsUpdateStatusCmd        `kong:"cmd,help='Update activation status'"`
	Delete              CDPActivationsDeleteCmd              `kong:"cmd,aliases='rm',help='Delete activation'"`
	Execute             CDPActivationsExecuteCmd             `kong:"cmd,help='Execute activation'"`
	Executions          CDPActivationsExecutionsCmd          `kong:"cmd,help='Get activation executions'"`
	ListByAudience      CDPActivationsListByAudienceCmd      `kong:"cmd,help='List activations by audience'"`
	ListBySegmentFolder CDPActivationsListBySegmentFolderCmd `kong:"cmd,help='List activations by segment folder'"`
	RunSegment          CDPActivationsRunSegmentCmd          `kong:"cmd,help='Run activation for segment'"`
	ListByParentSegment CDPActivationsListByParentSegmentCmd `kong:"cmd,help='List activations by parent segment'"`
	WorkflowProjects    CDPActivationsWorkflowProjectsCmd    `kong:"cmd,help='Get workflow projects for parent segment'"`
	Workflows           CDPActivationsWorkflowsCmd           `kong:"cmd,help='Get workflows for parent segment'"`
	MatchedActivations  CDPActivationsMatchedActivationsCmd  `kong:"cmd,help='Get matched activations for parent segment'"`
}

type CDPActivationsCreateCmd struct {
	SegmentID     string `kong:"arg,help='Segment ID'"`
	Name          string `kong:"arg,help='Activation name'"`
	Description   string `kong:"arg,help='Activation description'"`
	Configuration string `kong:"arg,help='Additional configuration (JSON)'"`
}

func (c *CDPActivationsCreateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.create", []string{c.SegmentID, c.Name}, func() {
		handleCDPActivationCreate(ctx.Context, ctx.Client, []string{c.SegmentID, c.Name, c.Description, c.Configuration}, ctx.GlobalFlags)
	})
}

type CDPActivationsCreateWithStructCmd struct {
	Name          string `kong:"arg,help='Activation name'"`
	Type          string `kong:"arg,help='Activation type'"`
	SegmentID     string `kong:"arg,help='Segment ID'"`
	Configuration string `kong:"arg,help='Configuration (JSON)'"`
	Description   string `kong:"optional,help='Activation description'"`
}

func (c *CDPActivationsCreateWithStructCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.create_with_struct", []string{c.Name, c.SegmentID}, func() {
		args := []string{c.Name, c.Type, c.SegmentID, c.Configuration}
		if c.Description != "" {
			args = append(args, c.Description)
		}
		handleCDPActivationCreateWithStruct(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPActivationsListCmd struct {
	Force bool `kong:"flag,help='Skip confirmation prompt'"`
}

func (c *CDPActivationsListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.list", []string{}, func() {
		handleCDPActivationListWithForce(ctx.Context, ctx.Client, ctx.GlobalFlags, c.Force)
	})
}

type CDPActivationsGetCmd struct {
	AudienceID   string `kong:"arg,help='Audience ID'"`
	SegmentID    string `kong:"arg,help='Segment ID'"`
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.get", []string{c.AudienceID, c.SegmentID, c.ActivationID}, func() {
		handleCDPActivationGet(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.ActivationID}, ctx.GlobalFlags)
	})
}

type CDPActivationsUpdateCmd struct {
	ActivationID string   `kong:"arg,help='Activation ID'"`
	Updates      []string `kong:"arg,help='Updates (key=value)'"`
}

func (c *CDPActivationsUpdateCmd) Run(ctx *CLIContext) error {
	args := append([]string{c.ActivationID}, c.Updates...)
	return runInstrumented(ctx, "cdp.activations.update", args, func() {
		handleCDPActivationUpdate(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPActivationsUpdateStatusCmd struct {
	ActivationID string `kong:"arg,help='Activation ID'"`
	Status       string `kong:"arg,help='New status'"`
}

func (c *CDPActivationsUpdateStatusCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.update_status", []string{c.ActivationID, c.Status}, func() {
		handleCDPActivationUpdateStatus(ctx.Context, ctx.Client, []string{c.ActivationID, c.Status}, ctx.GlobalFlags)
	})
}

type CDPActivationsDeleteCmd struct {
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsDeleteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.delete", []string{c.ActivationID}, func() {
		handleCDPActivationDelete(ctx.Context, ctx.Client, []string{c.ActivationID}, ctx.GlobalFlags)
	})
}

type CDPActivationsExecuteCmd struct {
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsExecuteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.execute", []string{c.ActivationID}, func() {
		handleCDPExecuteActivation(ctx.Context, ctx.Client, []string{c.ActivationID}, ctx.GlobalFlags)
	})
}

type CDPActivationsExecutionsCmd struct {
	AudienceID   string `kong:"arg,help='Audience ID'"`
	SegmentID    string `kong:"arg,help='Segment ID'"`
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsExecutionsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.executions", []string{c.AudienceID, c.SegmentID, c.ActivationID}, func() {
		handleCDPGetActivationExecutions(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.ActivationID}, ctx.GlobalFlags)
	})
}

type CDPActivationsListByAudienceCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPActivationsListByAudienceCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.list_by_audience", []string{c.AudienceID}, func() {
		handleCDPListActivationsByAudience(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPActivationsListBySegmentFolderCmd struct {
	FolderID string `kong:"arg,help='Segment folder ID'"`
}

func (c *CDPActivationsListBySegmentFolderCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.list_by_segment_folder", []string{c.FolderID}, func() {
		handleCDPListActivationsBySegmentFolder(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	})
}

type CDPActivationsRunSegmentCmd struct {
	ActivationID string `kong:"arg,help='Activation ID'"`
	SegmentID    string `kong:"arg,help='Segment ID'"`
}

func (c *CDPActivationsRunSegmentCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.run_segment", []string{c.ActivationID, c.SegmentID}, func() {
		handleCDPRunActivationForSegment(ctx.Context, ctx.Client, []string{c.ActivationID, c.SegmentID}, ctx.GlobalFlags)
	})
}

type CDPActivationsListByParentSegmentCmd struct {
	SegmentID string `kong:"arg,help='Parent segment ID'"`
}

func (c *CDPActivationsListByParentSegmentCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.list_by_parent_segment", []string{c.SegmentID}, func() {
		handleCDPListActivationsByParentSegment(ctx.Context, ctx.Client, []string{c.SegmentID}, ctx.GlobalFlags)
	})
}

type CDPActivationsWorkflowProjectsCmd struct {
	SegmentID string `kong:"arg,help='Parent segment ID'"`
}

func (c *CDPActivationsWorkflowProjectsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.workflow_projects", []string{c.SegmentID}, func() {
		handleCDPGetWorkflowProjectsForParentSegment(ctx.Context, ctx.Client, []string{c.SegmentID}, ctx.GlobalFlags)
	})
}

type CDPActivationsWorkflowsCmd struct {
	SegmentID           string `kong:"arg,help='Parent segment ID'"`
	WorkflowProjectName string `kong:"arg,help='Workflow project name'"`
}

func (c *CDPActivationsWorkflowsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.workflows", []string{c.SegmentID, c.WorkflowProjectName}, func() {
		handleCDPGetWorkflowsForParentSegment(ctx.Context, ctx.Client, []string{c.SegmentID, c.WorkflowProjectName}, ctx.GlobalFlags)
	})
}

type CDPActivationsMatchedActivationsCmd struct {
	SegmentID string `kong:"arg,help='Parent segment ID'"`
}

func (c *CDPActivationsMatchedActivationsCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activations.matched_activations", []string{c.SegmentID}, func() {
		handleCDPGetMatchedActivationsForParentSegment(ctx.Context, ctx.Client, []string{c.SegmentID}, ctx.GlobalFlags)
	})
}

// --- Folders ---

type CDPFoldersCmd struct {
	List         CDPFoldersListCmd         `kong:"cmd,aliases='ls',help='List folders in audience'"`
	Create       CDPFoldersCreateCmd       `kong:"cmd,help='Create folder in audience'"`
	Get          CDPFoldersGetCmd          `kong:"cmd,aliases='show',help='Get folder details'"`
	CreateEntity CDPFoldersCreateEntityCmd `kong:"cmd,help='Create entity folder'"`
	GetEntity    CDPFoldersGetEntityCmd    `kong:"cmd,help='Get entity folder'"`
	UpdateEntity CDPFoldersUpdateEntityCmd `kong:"cmd,help='Update entity folder'"`
	DeleteEntity CDPFoldersDeleteEntityCmd `kong:"cmd,help='Delete entity folder'"`
	GetEntities  CDPFoldersGetEntitiesCmd  `kong:"cmd,help='Get entities by folder'"`
}

type CDPFoldersListCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPFoldersListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.folders.list", []string{c.AudienceID}, func() {
		handleCDPListFolders(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPFoldersCreateCmd struct {
	AudienceID  string `kong:"arg,help='Audience ID'"`
	Name        string `kong:"arg,help='Folder name'"`
	Description string `kong:"optional,help='Folder description'"`
	ParentID    string `kong:"optional,help='Parent folder ID'"`
}

func (c *CDPFoldersCreateCmd) Run(ctx *CLIContext) error {
	args := []string{c.AudienceID, c.Name}
	return runInstrumented(ctx, "cdp.folders.create_audience", args, func() {
		realArgs := []string{c.AudienceID, c.Name}
		if c.Description != "" {
			realArgs = append(realArgs, c.Description)
		}
		if c.ParentID != "" {
			realArgs = append(realArgs, c.ParentID)
		}
		handleCDPCreateAudienceFolder(ctx.Context, ctx.Client, realArgs, ctx.GlobalFlags)
	})
}

type CDPFoldersGetCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	FolderID   string `kong:"arg,help='Folder ID'"`
}

func (c *CDPFoldersGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.folders.get_audience", []string{c.AudienceID, c.FolderID}, func() {
		handleCDPGetAudienceFolder(ctx.Context, ctx.Client, []string{c.AudienceID, c.FolderID}, ctx.GlobalFlags)
	})
}

type CDPFoldersCreateEntityCmd struct {
	Name        string `kong:"arg,help='Folder name'"`
	Description string `kong:"optional,help='Folder description'"`
	ParentID    string `kong:"optional,help='Parent folder ID'"`
}

func (c *CDPFoldersCreateEntityCmd) Run(ctx *CLIContext) error {
	args := []string{c.Name}
	return runInstrumented(ctx, "cdp.folders.create_entity", args, func() {
		realArgs := []string{c.Name}
		if c.Description != "" {
			realArgs = append(realArgs, c.Description)
		}
		if c.ParentID != "" {
			realArgs = append(realArgs, c.ParentID)
		}
		handleCDPCreateEntityFolder(ctx.Context, ctx.Client, realArgs, ctx.GlobalFlags)
	})
}

type CDPFoldersGetEntityCmd struct {
	FolderID string `kong:"arg,help='Folder ID'"`
}

func (c *CDPFoldersGetEntityCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.folders.get_entity", []string{c.FolderID}, func() {
		handleCDPGetEntityFolder(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	})
}

type CDPFoldersUpdateEntityCmd struct {
	FolderID    string `kong:"arg,help='Folder ID'"`
	Name        string `kong:"optional,help='New folder name'"`
	Description string `kong:"optional,help='New folder description'"`
	ParentID    string `kong:"optional,help='New parent folder ID'"`
}

func (c *CDPFoldersUpdateEntityCmd) Run(ctx *CLIContext) error {
	args := []string{c.FolderID}
	return runInstrumented(ctx, "cdp.folders.update_entity", args, func() {
		realArgs := []string{c.FolderID}
		if c.Name != "" {
			realArgs = append(realArgs, "name="+c.Name)
		}
		if c.Description != "" {
			realArgs = append(realArgs, "description="+c.Description)
		}
		if c.ParentID != "" {
			realArgs = append(realArgs, "parent_id="+c.ParentID)
		}
		handleCDPUpdateEntityFolder(ctx.Context, ctx.Client, realArgs, ctx.GlobalFlags)
	})
}

type CDPFoldersDeleteEntityCmd struct {
	FolderID string `kong:"arg,help='Folder ID'"`
}

func (c *CDPFoldersDeleteEntityCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.folders.delete_entity", []string{c.FolderID}, func() {
		handleCDPDeleteEntityFolder(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	})
}

type CDPFoldersGetEntitiesCmd struct {
	FolderID string `kong:"arg,help='Folder ID'"`
}

func (c *CDPFoldersGetEntitiesCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.folders.entities_by_folder", []string{c.FolderID}, func() {
		handleCDPGetEntitiesByFolder(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	})
}

// --- Tokens ---

type CDPTokensCmd struct {
	List         CDPTokensListCmd         `kong:"cmd,aliases='ls',help='List tokens'"`
	GetEntity    CDPTokensGetEntityCmd    `kong:"cmd,aliases='get,show',help='Get entity token details'"`
	UpdateEntity CDPTokensUpdateEntityCmd `kong:"cmd,help='Update entity token'"`
	DeleteEntity CDPTokensDeleteEntityCmd `kong:"cmd,aliases='rm',help='Delete entity token'"`
}

type CDPTokensListCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	Type       string `kong:"help='Filter by type'"`
	Status     string `kong:"help='Filter by status'"`
	Limit      int    `kong:"help='Limit results',default='100'"`
	Offset     int    `kong:"help='Offset for pagination',default='0'"`
}

func (c *CDPTokensListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.tokens.list", []string{c.AudienceID}, func() {
		handleCDPListTokens(ctx.Context, ctx.Client, c, ctx.GlobalFlags)
	})
}

type CDPTokensGetEntityCmd struct {
	TokenID string `kong:"arg,help='Token ID'"`
}

func (c *CDPTokensGetEntityCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.tokens.get_entity", []string{c.TokenID}, func() {
		handleCDPGetEntityToken(ctx.Context, ctx.Client, []string{c.TokenID}, ctx.GlobalFlags)
	})
}

type CDPTokensUpdateEntityCmd struct {
	TokenID string   `kong:"arg,help='Token ID'"`
	Updates []string `kong:"arg,help='Updates (key=value)'"`
}

func (c *CDPTokensUpdateEntityCmd) Run(ctx *CLIContext) error {
	args := append([]string{c.TokenID}, c.Updates...)
	return runInstrumented(ctx, "cdp.tokens.update_entity", args, func() {
		handleCDPUpdateEntityToken(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPTokensDeleteEntityCmd struct {
	TokenID string `kong:"arg,help='Token ID'"`
}

func (c *CDPTokensDeleteEntityCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.tokens.delete_entity", []string{c.TokenID}, func() {
		handleCDPDeleteEntityToken(ctx.Context, ctx.Client, []string{c.TokenID}, ctx.GlobalFlags)
	})
}

// --- Journeys ---

type CDPJourneysCmd struct {
	List             CDPJourneysListCmd             `kong:"cmd,aliases='ls',help='List journeys'"`
	Create           CDPJourneysCreateCmd           `kong:"cmd,help='Create a new journey'"`
	Get              CDPJourneysGetCmd              `kong:"cmd,aliases='show',help='Get journey details'"`
	Update           CDPJourneysUpdateCmd           `kong:"cmd,help='Update journey'"`
	Delete           CDPJourneysDeleteCmd           `kong:"cmd,aliases='rm',help='Delete journey'"`
	Detail           CDPJourneysDetailCmd           `kong:"cmd,help='Get journey detail'"`
	Duplicate        CDPJourneysDuplicateCmd        `kong:"cmd,help='Duplicate journey'"`
	Pause            CDPJourneysPauseCmd            `kong:"cmd,help='Pause journey'"`
	Resume           CDPJourneysResumeCmd           `kong:"cmd,help='Resume journey'"`
	Statistics       CDPJourneysStatisticsCmd       `kong:"cmd,aliases='stats',help='Get journey statistics'"`
	Customers        CDPJourneysCustomersCmd        `kong:"cmd,help='Get journey customers'"`
	StageCustomers   CDPJourneysStageCustomersCmd   `kong:"cmd,help='Get journey stage customers'"`
	ConversionSankey CDPJourneysConversionSankeyCmd `kong:"cmd,help='Get journey conversion sankey charts'"`
	ActivationSankey CDPJourneysActivationSankeyCmd `kong:"cmd,help='Get journey activation sankey charts'"`
	SegmentRules     CDPJourneysSegmentRulesCmd     `kong:"cmd,help='List journey segment rules'"`
	Behaviors        CDPJourneysBehaviorsCmd        `kong:"cmd,help='Get available behaviors for step'"`
	Templates        CDPJourneysTemplatesCmd        `kong:"cmd,help='Get activation templates for step'"`
	Activations      CDPJourneyActivationsCmd       `kong:"cmd,help='Journey activation management'"`
}

type CDPJourneysListCmd struct {
	FolderID string `kong:"arg,help='Folder ID'"`
}

func (c *CDPJourneysListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.list", []string{c.FolderID}, func() {
		handleCDPJourneyList(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	})
}

type CDPJourneysCreateCmd struct {
	RequestFile string `kong:"arg,help='JSON file with journey request data'"`
}

func (c *CDPJourneysCreateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.create", []string{}, func() {
		handleCDPJourneyCreate(ctx.Context, ctx.Client, []string{c.RequestFile}, ctx.GlobalFlags)
	})
}

type CDPJourneysGetCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
}

func (c *CDPJourneysGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.get", []string{c.JourneyID}, func() {
		handleCDPJourneyGet(ctx.Context, ctx.Client, []string{c.JourneyID}, ctx.GlobalFlags)
	})
}

type CDPJourneysUpdateCmd struct {
	JourneyID   string `kong:"arg,help='Journey ID'"`
	RequestFile string `kong:"arg,help='JSON file with journey update data'"`
}

func (c *CDPJourneysUpdateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.update", []string{c.JourneyID}, func() {
		handleCDPJourneyUpdate(ctx.Context, ctx.Client, []string{c.JourneyID, c.RequestFile}, ctx.GlobalFlags)
	})
}

type CDPJourneysDeleteCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
}

func (c *CDPJourneysDeleteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.delete", []string{c.JourneyID}, func() {
		handleCDPJourneyDelete(ctx.Context, ctx.Client, []string{c.JourneyID}, ctx.GlobalFlags)
	})
}

type CDPJourneysDetailCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
}

func (c *CDPJourneysDetailCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.detail", []string{c.JourneyID}, func() {
		handleCDPJourneyDetail(ctx.Context, ctx.Client, []string{c.JourneyID}, ctx.GlobalFlags)
	})
}

type CDPJourneysDuplicateCmd struct {
	RequestFile string `kong:"arg,help='JSON file with journey duplicate request data'"`
}

func (c *CDPJourneysDuplicateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.duplicate", []string{}, func() {
		handleCDPJourneyDuplicate(ctx.Context, ctx.Client, []string{c.RequestFile}, ctx.GlobalFlags)
	})
}

type CDPJourneysPauseCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
}

func (c *CDPJourneysPauseCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.pause", []string{c.JourneyID}, func() {
		handleCDPJourneyPause(ctx.Context, ctx.Client, []string{c.JourneyID}, ctx.GlobalFlags)
	})
}

type CDPJourneysResumeCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
}

func (c *CDPJourneysResumeCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.resume", []string{c.JourneyID}, func() {
		handleCDPJourneyResume(ctx.Context, ctx.Client, []string{c.JourneyID}, ctx.GlobalFlags)
	})
}

type CDPJourneysStatisticsCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
	From      string `kong:"flag,help='Start date (RFC3339 format)'"`
	To        string `kong:"flag,help='End date (RFC3339 format)'"`
}

func (c *CDPJourneysStatisticsCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID}
	if c.From != "" {
		args = append(args, "--from", c.From)
	}
	if c.To != "" {
		args = append(args, "--to", c.To)
	}
	return runInstrumented(ctx, "cdp.journeys.statistics", []string{c.JourneyID}, func() {
		handleCDPJourneyStatistics(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPJourneysCustomersCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
	Limit     int    `kong:"flag,help='Limit number of results',default='100'"`
	Offset    int    `kong:"flag,help='Offset for pagination',default='0'"`
}

func (c *CDPJourneysCustomersCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID, fmt.Sprintf("--limit=%d", c.Limit), fmt.Sprintf("--offset=%d", c.Offset)}
	return runInstrumented(ctx, "cdp.journeys.customers", []string{c.JourneyID}, func() {
		handleCDPJourneyCustomers(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPJourneysStageCustomersCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
	StageID   string `kong:"arg,help='Stage ID'"`
	Limit     int    `kong:"flag,help='Limit number of results',default='100'"`
	Offset    int    `kong:"flag,help='Offset for pagination',default='0'"`
}

func (c *CDPJourneysStageCustomersCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID, c.StageID, fmt.Sprintf("--limit=%d", c.Limit), fmt.Sprintf("--offset=%d", c.Offset)}
	return runInstrumented(ctx, "cdp.journeys.stage_customers", []string{c.JourneyID, c.StageID}, func() {
		handleCDPJourneyStageCustomers(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPJourneysConversionSankeyCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
	From      string `kong:"flag,help='Start date (RFC3339 format)'"`
	To        string `kong:"flag,help='End date (RFC3339 format)'"`
}

func (c *CDPJourneysConversionSankeyCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID}
	if c.From != "" {
		args = append(args, "--from", c.From)
	}
	if c.To != "" {
		args = append(args, "--to", c.To)
	}
	return runInstrumented(ctx, "cdp.journeys.conversion_sankey", []string{c.JourneyID}, func() {
		handleCDPJourneyConversionSankey(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPJourneysActivationSankeyCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
	From      string `kong:"flag,help='Start date (RFC3339 format)'"`
	To        string `kong:"flag,help='End date (RFC3339 format)'"`
}

func (c *CDPJourneysActivationSankeyCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID}
	if c.From != "" {
		args = append(args, "--from", c.From)
	}
	if c.To != "" {
		args = append(args, "--to", c.To)
	}
	return runInstrumented(ctx, "cdp.journeys.activation_sankey", []string{c.JourneyID}, func() {
		handleCDPJourneyActivationSankey(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPJourneysSegmentRulesCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPJourneysSegmentRulesCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.segment_rules", []string{c.AudienceID}, func() {
		handleCDPJourneySegmentRules(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	})
}

type CDPJourneysBehaviorsCmd struct {
	JourneyID string  `kong:"arg,help='Journey ID'"`
	StepID    *string `kong:"flag,help='Step ID (optional)'"`
}

func (c *CDPJourneysBehaviorsCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID}
	if c.StepID != nil {
		args = append(args, "--step-id", *c.StepID)
	}
	return runInstrumented(ctx, "cdp.journeys.behaviors", []string{c.JourneyID}, func() {
		handleCDPJourneyBehaviors(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPJourneysTemplatesCmd struct {
	JourneyID string  `kong:"arg,help='Journey ID'"`
	StepID    *string `kong:"flag,help='Step ID (optional)'"`
}

func (c *CDPJourneysTemplatesCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID}
	if c.StepID != nil {
		args = append(args, "--step-id", *c.StepID)
	}
	return runInstrumented(ctx, "cdp.journeys.templates", []string{c.JourneyID}, func() {
		handleCDPJourneyTemplates(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type CDPJourneyActivationsCmd struct {
	List   CDPJourneyActivationsListCmd   `kong:"cmd,aliases='ls',help='List journey activations'"`
	Create CDPJourneyActivationsCreateCmd `kong:"cmd,help='Create journey activation'"`
	Get    CDPJourneyActivationsGetCmd    `kong:"cmd,aliases='show',help='Get journey activation'"`
	Update CDPJourneyActivationsUpdateCmd `kong:"cmd,help='Update journey activation'"`
}

type CDPJourneyActivationsListCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
}

func (c *CDPJourneyActivationsListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.activations.list", []string{c.JourneyID}, func() {
		handleCDPJourneyActivationList(ctx.Context, ctx.Client, []string{c.JourneyID}, ctx.GlobalFlags)
	})
}

type CDPJourneyActivationsCreateCmd struct {
	JourneyID   string `kong:"arg,help='Journey ID'"`
	RequestFile string `kong:"arg,help='JSON file with activation request data'"`
}

func (c *CDPJourneyActivationsCreateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.activations.create", []string{c.JourneyID}, func() {
		handleCDPJourneyActivationCreate(ctx.Context, ctx.Client, []string{c.JourneyID, c.RequestFile}, ctx.GlobalFlags)
	})
}

type CDPJourneyActivationsGetCmd struct {
	JourneyID        string `kong:"arg,help='Journey ID'"`
	ActivationStepID string `kong:"arg,help='Activation Step ID'"`
}

func (c *CDPJourneyActivationsGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.activations.get", []string{c.JourneyID, c.ActivationStepID}, func() {
		handleCDPJourneyActivationGet(ctx.Context, ctx.Client, []string{c.JourneyID, c.ActivationStepID}, ctx.GlobalFlags)
	})
}

type CDPJourneyActivationsUpdateCmd struct {
	JourneyID        string `kong:"arg,help='Journey ID'"`
	ActivationStepID string `kong:"arg,help='Activation Step ID'"`
	RequestFile      string `kong:"arg,help='JSON file with activation update data'"`
}

func (c *CDPJourneyActivationsUpdateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.journeys.activations.update", []string{c.JourneyID, c.ActivationStepID}, func() {
		handleCDPJourneyActivationUpdate(ctx.Context, ctx.Client, []string{c.JourneyID, c.ActivationStepID, c.RequestFile}, ctx.GlobalFlags)
	})
}

// --- Activation Templates ---

type CDPActivationTemplatesCmd struct {
	List   CDPActivationTemplatesListCmd   `kong:"cmd,aliases='ls',help='List activation templates by parent segment'"`
	Create CDPActivationTemplatesCreateCmd `kong:"cmd,help='Create a new activation template'"`
	Get    CDPActivationTemplatesGetCmd    `kong:"cmd,aliases='show',help='Get activation template details'"`
	Update CDPActivationTemplatesUpdateCmd `kong:"cmd,help='Update activation template'"`
	Delete CDPActivationTemplatesDeleteCmd `kong:"cmd,aliases='rm',help='Delete activation template'"`
}

type CDPActivationTemplatesListCmd struct {
	ParentSegmentID string `kong:"arg,help='Parent Segment ID'"`
}

func (c *CDPActivationTemplatesListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activation_templates.list", []string{c.ParentSegmentID}, func() {
		handleCDPActivationTemplateList(ctx.Context, ctx.Client, []string{c.ParentSegmentID}, ctx.GlobalFlags)
	})
}

type CDPActivationTemplatesCreateCmd struct {
	RequestFile string `kong:"arg,help='JSON file with activation template request data'"`
}

func (c *CDPActivationTemplatesCreateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activation_templates.create", []string{}, func() {
		handleCDPActivationTemplateCreate(ctx.Context, ctx.Client, []string{c.RequestFile}, ctx.GlobalFlags)
	})
}

type CDPActivationTemplatesGetCmd struct {
	TemplateID string `kong:"arg,help='Activation Template ID'"`
}

func (c *CDPActivationTemplatesGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activation_templates.get", []string{c.TemplateID}, func() {
		handleCDPActivationTemplateGet(ctx.Context, ctx.Client, []string{c.TemplateID}, ctx.GlobalFlags)
	})
}

type CDPActivationTemplatesUpdateCmd struct {
	TemplateID  string `kong:"arg,help='Activation Template ID'"`
	RequestFile string `kong:"arg,help='JSON file with activation template update data'"`
}

func (c *CDPActivationTemplatesUpdateCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activation_templates.update", []string{c.TemplateID}, func() {
		handleCDPActivationTemplateUpdate(ctx.Context, ctx.Client, []string{c.TemplateID, c.RequestFile}, ctx.GlobalFlags)
	})
}

type CDPActivationTemplatesDeleteCmd struct {
	TemplateID string `kong:"arg,help='Activation Template ID'"`
}

func (c *CDPActivationTemplatesDeleteCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "cdp.activation_templates.delete", []string{c.TemplateID}, func() {
		handleCDPActivationTemplateDelete(ctx.Context, ctx.Client, []string{c.TemplateID}, ctx.GlobalFlags)
	})
}
