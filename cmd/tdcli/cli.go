package main

import (
	"context"
	"fmt"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Global CLI structure
type CLI struct {
	// Global flags
	APIKey  string `kong:"help='Treasure Data API key (format: account_id/api_key)',env='TD_API_KEY'"`
	Region  string `kong:"help='API region (us, eu, tokyo, ap02)',default='us'"`
	Format  string `kong:"help='Output format (json, table, csv)',default='table',enum='json,table,csv'"`
	Output  string `kong:"help='Output to file'"`
	Verbose bool   `kong:"short='v',help='Verbose output'"`

	// Commands
	Version   VersionCmd   `kong:"cmd,help='Show version'"`
	Config    ConfigCmd    `kong:"cmd,help='Configuration management'"`
	Databases DatabasesCmd `kong:"cmd,aliases='db',help='Database management'"`
	Tables    TablesCmd    `kong:"cmd,aliases='table',help='Table management'"`
	Queries   QueriesCmd   `kong:"cmd,aliases='query,q',help='Query execution'"`
	Jobs      JobsCmd      `kong:"cmd,aliases='job',help='Job management'"`
	Users     UsersCmd     `kong:"cmd,aliases='user',help='User management'"`
	Perms     PermsCmd     `kong:"cmd,aliases='permissions,acl',help='Access control and permissions'"`
	Results   ResultsCmd   `kong:"cmd,aliases='result',help='Query results management'"`
	Import    ImportCmd    `kong:"cmd,aliases='bulk-import',help='Bulk data import'"`
	CDP       CDPCmd       `kong:"cmd,help='Customer Data Platform (CDP) management'"`
	Workflow  WorkflowCmd  `kong:"cmd,aliases='wf',help='Workflow management'"`
}

// Version command
type VersionCmd struct{}

func (v *VersionCmd) Run(ctx *CLIContext) error {
	fmt.Printf("tdcli version %s\n", version)
	fmt.Printf("commit: %s\n", commit)
	fmt.Printf("built: %s\n", date)
	return nil
}

// Database commands
type DatabasesCmd struct {
	List   DatabasesListCmd   `kong:"cmd,aliases='ls',help='List databases'"`
	Get    DatabasesGetCmd    `kong:"cmd,aliases='show',help='Get database details'"`
	Create DatabasesCreateCmd `kong:"cmd,help='Create a database'"`
	Delete DatabasesDeleteCmd `kong:"cmd,aliases='rm',help='Delete a database'"`
	Update DatabasesUpdateCmd `kong:"cmd,help='Update database properties'"`
}

type DatabasesListCmd struct{}

func (d *DatabasesListCmd) Run(ctx *CLIContext) error {
	handleDatabaseList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type DatabasesGetCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesGetCmd) Run(ctx *CLIContext) error {
	handleDatabaseGet(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	return nil
}

type DatabasesCreateCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesCreateCmd) Run(ctx *CLIContext) error {
	handleDatabaseCreate(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	return nil
}

type DatabasesDeleteCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesDeleteCmd) Run(ctx *CLIContext) error {
	handleDatabaseDelete(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	return nil
}

type DatabasesUpdateCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesUpdateCmd) Run(ctx *CLIContext) error {
	handleDatabaseUpdate(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	return nil
}

// Table commands
type TablesCmd struct {
	List          TablesListCmd          `kong:"cmd,aliases='ls',help='List tables in database'"`
	Get           TablesGetCmd           `kong:"cmd,aliases='show',help='Get table details'"`
	Create        TablesCreateCmd        `kong:"cmd,help='Create a table'"`
	Delete        TablesDeleteCmd        `kong:"cmd,aliases='rm',help='Delete a table'"`
	Swap          TablesSwapCmd          `kong:"cmd,help='Swap two tables'"`
	Rename        TablesRenameCmd        `kong:"cmd,aliases='mv',help='Rename a table'"`
	PartialDelete TablesPartialDeleteCmd `kong:"cmd,help='Delete partial data'"`
}

type TablesListCmd struct {
	Database string `kong:"arg,help='Database name'"`
}

func (t *TablesListCmd) Run(ctx *CLIContext) error {
	handleTableList(ctx.Context, ctx.Client, []string{t.Database}, ctx.GlobalFlags)
	return nil
}

type TablesGetCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (t *TablesGetCmd) Run(ctx *CLIContext) error {
	handleTableGet(ctx.Context, ctx.Client, []string{t.Database, t.Table}, ctx.GlobalFlags)
	return nil
}

type TablesCreateCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (t *TablesCreateCmd) Run(ctx *CLIContext) error {
	handleTableCreate(ctx.Context, ctx.Client, []string{t.Database, t.Table}, ctx.GlobalFlags)
	return nil
}

type TablesDeleteCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (t *TablesDeleteCmd) Run(ctx *CLIContext) error {
	handleTableDelete(ctx.Context, ctx.Client, []string{t.Database, t.Table}, ctx.GlobalFlags)
	return nil
}

type TablesSwapCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table1   string `kong:"arg,help='First table name'"`
	Table2   string `kong:"arg,help='Second table name'"`
}

func (t *TablesSwapCmd) Run(ctx *CLIContext) error {
	handleTableSwap(ctx.Context, ctx.Client, []string{t.Database, t.Table1, t.Table2}, ctx.GlobalFlags)
	return nil
}

type TablesRenameCmd struct {
	Database string `kong:"arg,help='Database name'"`
	OldName  string `kong:"arg,help='Current table name'"`
	NewName  string `kong:"arg,help='New table name'"`
}

func (t *TablesRenameCmd) Run(ctx *CLIContext) error {
	handleTableRename(ctx.Context, ctx.Client, []string{t.Database, t.OldName, t.NewName}, ctx.GlobalFlags)
	return nil
}

type TablesPartialDeleteCmd struct {
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (t *TablesPartialDeleteCmd) Run(ctx *CLIContext) error {
	handleTablePartialDelete(ctx.Context, ctx.Client, []string{t.Database, t.Table}, ctx.GlobalFlags)
	return nil
}

// Query commands
type QueriesCmd struct {
	Submit QuerySubmitCmd `kong:"cmd,aliases='run',help='Submit a query for execution'"`
	Status QueryStatusCmd `kong:"cmd,help='Check query execution status'"`
	Result QueryResultCmd `kong:"cmd,aliases='results',help='Get query results'"`
	List   QueryListCmd   `kong:"cmd,aliases='ls',help='List recent queries'"`
	Cancel QueryCancelCmd `kong:"cmd,help='Cancel a running query'"`
}

type QuerySubmitCmd struct {
	Query    string `kong:"arg,help='SQL query to execute'"`
	Database string `kong:"required,help='Database to run query against'"`
	Engine   string `kong:"help='Query engine: trino (default) or hive',default='trino',enum='trino,hive,presto'"`
	Priority int    `kong:"help='Query priority (0-2)',default=0"`
	Wait     bool   `kong:"help='Wait for query completion'"`
	Timeout  int    `kong:"help='Wait timeout in seconds',default=300"`
}

func (q *QuerySubmitCmd) Run(ctx *CLIContext) error {
	// Set database in global flags for compatibility
	ctx.GlobalFlags.Database = q.Database
	ctx.GlobalFlags.Priority = q.Priority
	handleQuerySubmit(ctx.Context, ctx.Client, []string{q.Query}, ctx.GlobalFlags)
	return nil
}

type QueryStatusCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (q *QueryStatusCmd) Run(ctx *CLIContext) error {
	handleQueryStatus(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
	return nil
}

type QueryResultCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
	Limit int    `kong:"help='Limit number of result rows'"`
}

func (q *QueryResultCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Limit = q.Limit
	handleQueryResult(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
	return nil
}

type QueryListCmd struct {
	Status string `kong:"help='Filter by job status'"`
}

func (q *QueryListCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Status = q.Status
	handleQueryList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type QueryCancelCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (q *QueryCancelCmd) Run(ctx *CLIContext) error {
	handleQueryCancel(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
	return nil
}

// Job commands
type JobsCmd struct {
	List   JobsListCmd   `kong:"cmd,aliases='ls',help='List jobs'"`
	Get    JobsGetCmd    `kong:"cmd,aliases='show',help='Get job details'"`
	Cancel JobsCancelCmd `kong:"cmd,aliases='kill',help='Cancel a running job'"`
}

type JobsListCmd struct {
	Status string `kong:"help='Filter by job status'"`
}

func (j *JobsListCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Status = j.Status
	handleJobList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type JobsGetCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (j *JobsGetCmd) Run(ctx *CLIContext) error {
	handleJobGet(ctx.Context, ctx.Client, []string{j.JobID}, ctx.GlobalFlags)
	return nil
}

type JobsCancelCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (j *JobsCancelCmd) Run(ctx *CLIContext) error {
	handleJobCancel(ctx.Context, ctx.Client, []string{j.JobID}, ctx.GlobalFlags)
	return nil
}

// User commands
type UsersCmd struct {
	List UsersListCmd `kong:"cmd,aliases='ls',help='List users'"`
	Get  UsersGetCmd  `kong:"cmd,aliases='show',help='Get user details'"`
}

type UsersListCmd struct{}

func (u *UsersListCmd) Run(ctx *CLIContext) error {
	handleUserList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type UsersGetCmd struct {
	UserID string `kong:"arg,help='User ID'"`
}

func (u *UsersGetCmd) Run(ctx *CLIContext) error {
	handleUserGet(ctx.Context, ctx.Client, []string{u.UserID}, ctx.GlobalFlags)
	return nil
}

// Permissions commands
type PermsCmd struct {
	Policies PermsPoliciesCmd `kong:"cmd,help='Policy management'"`
	Groups   PermsGroupsCmd   `kong:"cmd,help='Policy group management'"`
	Users    PermsUsersCmd    `kong:"cmd,help='Access control user management'"`
}

type PermsPoliciesCmd struct {
	List   PermsPoliciesListCmd   `kong:"cmd,aliases='ls',help='List all policies'"`
	Get    PermsPoliciesGetCmd    `kong:"cmd,aliases='show',help='Get policy details'"`
	Create PermsPoliciesCreateCmd `kong:"cmd,help='Create a new policy'"`
	Delete PermsPoliciesDeleteCmd `kong:"cmd,aliases='rm',help='Delete a policy'"`
}

type PermsPoliciesListCmd struct{}

func (p *PermsPoliciesListCmd) Run(ctx *CLIContext) error {
	handlePolicyList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type PermsPoliciesGetCmd struct {
	PolicyID int `kong:"arg,help='Policy ID'"`
}

func (p *PermsPoliciesGetCmd) Run(ctx *CLIContext) error {
	handlePolicyGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.PolicyID)}, ctx.GlobalFlags)
	return nil
}

type PermsPoliciesCreateCmd struct {
	Name        string `kong:"arg,help='Policy name'"`
	Description string `kong:"help='Policy description'"`
}

func (p *PermsPoliciesCreateCmd) Run(ctx *CLIContext) error {
	args := []string{p.Name}
	if p.Description != "" {
		args = append(args, p.Description)
	}
	handlePolicyCreate(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type PermsPoliciesDeleteCmd struct {
	PolicyID int `kong:"arg,help='Policy ID'"`
}

func (p *PermsPoliciesDeleteCmd) Run(ctx *CLIContext) error {
	handlePolicyDelete(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.PolicyID)}, ctx.GlobalFlags)
	return nil
}

type PermsGroupsCmd struct {
	List   PermsGroupsListCmd   `kong:"cmd,aliases='ls',help='List all policy groups'"`
	Get    PermsGroupsGetCmd    `kong:"cmd,aliases='show',help='Get policy group details'"`
	Create PermsGroupsCreateCmd `kong:"cmd,help='Create a new policy group'"`
	Delete PermsGroupsDeleteCmd `kong:"cmd,aliases='rm',help='Delete a policy group'"`
}

type PermsGroupsListCmd struct{}

func (p *PermsGroupsListCmd) Run(ctx *CLIContext) error {
	handlePolicyGroupList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type PermsGroupsGetCmd struct {
	GroupID string `kong:"arg,help='Policy group ID'"`
}

func (p *PermsGroupsGetCmd) Run(ctx *CLIContext) error {
	handlePolicyGroupGet(ctx.Context, ctx.Client, []string{p.GroupID}, ctx.GlobalFlags)
	return nil
}

type PermsGroupsCreateCmd struct {
	Name string `kong:"arg,help='Policy group name'"`
}

func (p *PermsGroupsCreateCmd) Run(ctx *CLIContext) error {
	handlePolicyGroupCreate(ctx.Context, ctx.Client, []string{p.Name}, ctx.GlobalFlags)
	return nil
}

type PermsGroupsDeleteCmd struct {
	GroupID string `kong:"arg,help='Policy group ID'"`
}

func (p *PermsGroupsDeleteCmd) Run(ctx *CLIContext) error {
	handlePolicyGroupDelete(ctx.Context, ctx.Client, []string{p.GroupID}, ctx.GlobalFlags)
	return nil
}

type PermsUsersCmd struct {
	List PermsUsersListCmd `kong:"cmd,aliases='ls',help='List access control users'"`
	Get  PermsUsersGetCmd  `kong:"cmd,aliases='show',help='Get user access control details'"`
}

type PermsUsersListCmd struct {
	WithDetails bool `kong:"help='Include user email and name details',default=true"`
}

func (p *PermsUsersListCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.WithDetails = p.WithDetails
	handleAccessControlUserList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type PermsUsersGetCmd struct {
	UserID int `kong:"arg,help='User ID'"`
}

func (p *PermsUsersGetCmd) Run(ctx *CLIContext) error {
	handleAccessControlUserGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.UserID)}, ctx.GlobalFlags)
	return nil
}

// Results commands
type ResultsCmd struct {
	Get ResultsGetCmd `kong:"cmd,aliases='show',help='Get query results'"`
}

type ResultsGetCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
	Limit int    `kong:"help='Limit number of rows'"`
}

func (r *ResultsGetCmd) Run(ctx *CLIContext) error {
	ctx.GlobalFlags.Limit = r.Limit
	handleResultGet(ctx.Context, ctx.Client, []string{r.JobID}, ctx.GlobalFlags)
	return nil
}

// Import (Bulk Import) commands
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
	handleBulkImportList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type ImportGetCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportGetCmd) Run(ctx *CLIContext) error {
	handleBulkImportGet(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	return nil
}

type ImportCreateCmd struct {
	Session  string `kong:"arg,help='Session name'"`
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (i *ImportCreateCmd) Run(ctx *CLIContext) error {
	handleBulkImportCreate(ctx.Context, ctx.Client, []string{i.Session, i.Database, i.Table}, ctx.GlobalFlags)
	return nil
}

type ImportDeleteCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportDeleteCmd) Run(ctx *CLIContext) error {
	handleBulkImportDelete(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	return nil
}

type ImportUploadCmd struct {
	Session  string `kong:"arg,help='Session name'"`
	PartName string `kong:"arg,help='Part name'"`
	FilePath string `kong:"arg,help='File path'"`
}

func (i *ImportUploadCmd) Run(ctx *CLIContext) error {
	handleBulkImportUpload(ctx.Context, ctx.Client, []string{i.Session, i.PartName, i.FilePath}, ctx.GlobalFlags)
	return nil
}

type ImportCommitCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportCommitCmd) Run(ctx *CLIContext) error {
	handleBulkImportCommit(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	return nil
}

type ImportPerformCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportPerformCmd) Run(ctx *CLIContext) error {
	handleBulkImportPerform(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	return nil
}

type ImportFreezeCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportFreezeCmd) Run(ctx *CLIContext) error {
	handleBulkImportFreeze(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	return nil
}

type ImportUnfreezeCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportUnfreezeCmd) Run(ctx *CLIContext) error {
	handleBulkImportUnfreeze(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	return nil
}

type ImportPartsCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportPartsCmd) Run(ctx *CLIContext) error {
	handleBulkImportParts(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	return nil
}

// Flags struct for compatibility with existing handlers
type Flags struct {
	APIKey      string
	Region      string
	Format      string
	Output      string
	Verbose     bool
	Database    string
	Status      string
	Priority    int
	Limit       int
	WithDetails bool
}

// Context structure for command execution
type CLIContext struct {
	Context     context.Context
	Client      *td.Client
	GlobalFlags Flags
}

// CDP commands
type CDPCmd struct {
	Segments    CDPSegmentsCmd    `kong:"cmd,aliases='segment',help='CDP segment management'"`
	Audiences   CDPAudiencesCmd   `kong:"cmd,aliases='audience',help='CDP audience management'"`
	Activations CDPActivationsCmd `kong:"cmd,aliases='activation',help='CDP activation management'"`
	Folders     CDPFoldersCmd     `kong:"cmd,aliases='folder',help='CDP folder management'"`
	Tokens      CDPTokensCmd      `kong:"cmd,aliases='token',help='CDP token management'"`
}

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
	handleCDPSegmentCreate(ctx.Context, ctx.Client, []string{c.AudienceID, c.Name, c.Description, c.Query}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsListCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPSegmentsListCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentList(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsGetCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
}

func (c *CDPSegmentsGetCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentGet(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsUpdateCmd struct {
	AudienceID string   `kong:"arg,help='Audience ID'"`
	SegmentID  string   `kong:"arg,help='Segment ID'"`
	Updates    []string `kong:"arg,help='Updates (key=value)'"`
}

func (c *CDPSegmentsUpdateCmd) Run(ctx *CLIContext) error {
	args := append([]string{c.AudienceID, c.SegmentID}, c.Updates...)
	handleCDPSegmentUpdate(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsDeleteCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
}

func (c *CDPSegmentsDeleteCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentDelete(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsFoldersCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	FolderID   string `kong:"arg,help='Folder ID'"`
}

func (c *CDPSegmentsFoldersCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentFolders(ctx.Context, ctx.Client, []string{c.AudienceID, c.FolderID}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsQueryCmd struct {
	AudienceID    string `kong:"arg,help='Audience ID'"`
	SegmentRules  string `kong:"arg,help='Segment rules JSON'"`
}

func (c *CDPSegmentsQueryCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentQuery(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentRules}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsNewQueryCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
	Query      string `kong:"arg,help='Query text'"`
}

func (c *CDPSegmentsNewQueryCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentNewQuery(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.Query}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsQueryStatusCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
	QueryID    string `kong:"arg,help='Query ID'"`
}

func (c *CDPSegmentsQueryStatusCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentQueryStatus(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.QueryID}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsKillQueryCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
	QueryID    string `kong:"arg,help='Query ID'"`
}

func (c *CDPSegmentsKillQueryCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentKillQuery(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.QueryID}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsCustomersCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
	Limit      int    `kong:"help='Limit number of results',default='100'"`
	Offset     int    `kong:"help='Offset for pagination',default='0'"`
	Fields     string `kong:"help='Comma-separated list of fields to include'"`
}

func (c *CDPSegmentsCustomersCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentCustomers(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID}, ctx.GlobalFlags)
	return nil
}

type CDPSegmentsStatisticsCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	SegmentID  string `kong:"arg,help='Segment ID'"`
}

func (c *CDPSegmentsStatisticsCmd) Run(ctx *CLIContext) error {
	handleCDPSegmentStatistics(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID}, ctx.GlobalFlags)
	return nil
}

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
	handleCDPAudienceCreate(ctx.Context, ctx.Client, []string{c.Name, c.Description, c.SegmentIDs}, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesListCmd struct{}

func (c *CDPAudiencesListCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesGetCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesGetCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceGet(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesDeleteCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesDeleteCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceDelete(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesBehaviorsCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesBehaviorsCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceBehaviors(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesRunCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesRunCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceRun(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesExecutionsCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesExecutionsCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceExecutions(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesStatisticsCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPAudiencesStatisticsCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceStatistics(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesSampleValuesCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	Column     string `kong:"arg,help='Column name'"`
}

func (c *CDPAudiencesSampleValuesCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceSampleValues(ctx.Context, ctx.Client, []string{c.AudienceID, c.Column}, ctx.GlobalFlags)
	return nil
}

type CDPAudiencesBehaviorSamplesCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	BehaviorID string `kong:"arg,help='Behavior ID'"`
	Column     string `kong:"arg,help='Column name'"`
}

func (c *CDPAudiencesBehaviorSamplesCmd) Run(ctx *CLIContext) error {
	handleCDPAudienceBehaviorSamples(ctx.Context, ctx.Client, []string{c.AudienceID, c.BehaviorID, c.Column}, ctx.GlobalFlags)
	return nil
}

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
	handleCDPActivationCreate(ctx.Context, ctx.Client, []string{c.SegmentID, c.Name, c.Description, c.Configuration}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsCreateWithStructCmd struct {
	Name          string `kong:"arg,help='Activation name'"`
	Type          string `kong:"arg,help='Activation type'"`
	SegmentID     string `kong:"arg,help='Segment ID'"`
	Configuration string `kong:"arg,help='Configuration (JSON)'"`
	Description   string `kong:"optional,help='Activation description'"`
}

func (c *CDPActivationsCreateWithStructCmd) Run(ctx *CLIContext) error {
	args := []string{c.Name, c.Type, c.SegmentID, c.Configuration}
	if c.Description != "" {
		args = append(args, c.Description)
	}
	handleCDPActivationCreateWithStruct(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPActivationsListCmd struct {
	Force bool `kong:"flag,help='Skip confirmation prompt'"`
}

func (c *CDPActivationsListCmd) Run(ctx *CLIContext) error {
	handleCDPActivationListWithForce(ctx.Context, ctx.Client, ctx.GlobalFlags, c.Force)
	return nil
}

type CDPActivationsGetCmd struct {
	AudienceID   string `kong:"arg,help='Audience ID'"`
	SegmentID    string `kong:"arg,help='Segment ID'"`
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsGetCmd) Run(ctx *CLIContext) error {
	handleCDPActivationGet(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.ActivationID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsUpdateCmd struct {
	ActivationID string   `kong:"arg,help='Activation ID'"`
	Updates      []string `kong:"arg,help='Updates (key=value)'"`
}

func (c *CDPActivationsUpdateCmd) Run(ctx *CLIContext) error {
	args := append([]string{c.ActivationID}, c.Updates...)
	handleCDPActivationUpdate(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPActivationsUpdateStatusCmd struct {
	ActivationID string `kong:"arg,help='Activation ID'"`
	Status       string `kong:"arg,help='New status'"`
}

func (c *CDPActivationsUpdateStatusCmd) Run(ctx *CLIContext) error {
	handleCDPActivationUpdateStatus(ctx.Context, ctx.Client, []string{c.ActivationID, c.Status}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsDeleteCmd struct {
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsDeleteCmd) Run(ctx *CLIContext) error {
	handleCDPActivationDelete(ctx.Context, ctx.Client, []string{c.ActivationID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsExecuteCmd struct {
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsExecuteCmd) Run(ctx *CLIContext) error {
	handleCDPExecuteActivation(ctx.Context, ctx.Client, []string{c.ActivationID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsExecutionsCmd struct {
	AudienceID   string `kong:"arg,help='Audience ID'"`
	SegmentID    string `kong:"arg,help='Segment ID'"`
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsExecutionsCmd) Run(ctx *CLIContext) error {
	handleCDPGetActivationExecutions(ctx.Context, ctx.Client, []string{c.AudienceID, c.SegmentID, c.ActivationID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsListByAudienceCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
}

func (c *CDPActivationsListByAudienceCmd) Run(ctx *CLIContext) error {
	handleCDPListActivationsByAudience(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsListBySegmentFolderCmd struct {
	FolderID string `kong:"arg,help='Segment folder ID'"`
}

func (c *CDPActivationsListBySegmentFolderCmd) Run(ctx *CLIContext) error {
	handleCDPListActivationsBySegmentFolder(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsRunSegmentCmd struct {
	ActivationID string `kong:"arg,help='Activation ID'"`
	SegmentID    string `kong:"arg,help='Segment ID'"`
}

func (c *CDPActivationsRunSegmentCmd) Run(ctx *CLIContext) error {
	handleCDPRunActivationForSegment(ctx.Context, ctx.Client, []string{c.ActivationID, c.SegmentID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsListByParentSegmentCmd struct {
	SegmentID string `kong:"arg,help='Parent segment ID'"`
}

func (c *CDPActivationsListByParentSegmentCmd) Run(ctx *CLIContext) error {
	handleCDPListActivationsByParentSegment(ctx.Context, ctx.Client, []string{c.SegmentID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsWorkflowProjectsCmd struct {
	SegmentID string `kong:"arg,help='Parent segment ID'"`
}

func (c *CDPActivationsWorkflowProjectsCmd) Run(ctx *CLIContext) error {
	handleCDPGetWorkflowProjectsForParentSegment(ctx.Context, ctx.Client, []string{c.SegmentID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsWorkflowsCmd struct {
	SegmentID string `kong:"arg,help='Parent segment ID'"`
}

func (c *CDPActivationsWorkflowsCmd) Run(ctx *CLIContext) error {
	handleCDPGetWorkflowsForParentSegment(ctx.Context, ctx.Client, []string{c.SegmentID}, ctx.GlobalFlags)
	return nil
}

type CDPActivationsMatchedActivationsCmd struct {
	SegmentID string `kong:"arg,help='Parent segment ID'"`
}

func (c *CDPActivationsMatchedActivationsCmd) Run(ctx *CLIContext) error {
	handleCDPGetMatchedActivationsForParentSegment(ctx.Context, ctx.Client, []string{c.SegmentID}, ctx.GlobalFlags)
	return nil
}

// CDP Folders commands
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
	handleCDPListFolders(ctx.Context, ctx.Client, []string{c.AudienceID}, ctx.GlobalFlags)
	return nil
}

type CDPFoldersCreateCmd struct {
	AudienceID  string `kong:"arg,help='Audience ID'"`
	Name        string `kong:"arg,help='Folder name'"`
	Description string `kong:"optional,help='Folder description'"`
	ParentID    string `kong:"optional,help='Parent folder ID'"`
}

func (c *CDPFoldersCreateCmd) Run(ctx *CLIContext) error {
	args := []string{c.AudienceID, c.Name}
	if c.Description != "" {
		args = append(args, c.Description)
	}
	if c.ParentID != "" {
		args = append(args, c.ParentID)
	}
	handleCDPCreateAudienceFolder(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPFoldersGetCmd struct {
	AudienceID string `kong:"arg,help='Audience ID'"`
	FolderID   string `kong:"arg,help='Folder ID'"`
}

func (c *CDPFoldersGetCmd) Run(ctx *CLIContext) error {
	handleCDPGetAudienceFolder(ctx.Context, ctx.Client, []string{c.AudienceID, c.FolderID}, ctx.GlobalFlags)
	return nil
}

type CDPFoldersCreateEntityCmd struct {
	Name        string `kong:"arg,help='Folder name'"`
	Description string `kong:"optional,help='Folder description'"`
	ParentID    string `kong:"optional,help='Parent folder ID'"`
}

func (c *CDPFoldersCreateEntityCmd) Run(ctx *CLIContext) error {
	args := []string{c.Name}
	if c.Description != "" {
		args = append(args, c.Description)
	}
	if c.ParentID != "" {
		args = append(args, c.ParentID)
	}
	handleCDPCreateEntityFolder(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPFoldersGetEntityCmd struct {
	FolderID string `kong:"arg,help='Folder ID'"`
}

func (c *CDPFoldersGetEntityCmd) Run(ctx *CLIContext) error {
	handleCDPGetEntityFolder(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	return nil
}

type CDPFoldersUpdateEntityCmd struct {
	FolderID    string `kong:"arg,help='Folder ID'"`
	Name        string `kong:"optional,help='New folder name'"`
	Description string `kong:"optional,help='New folder description'"`
	ParentID    string `kong:"optional,help='New parent folder ID'"`
}

func (c *CDPFoldersUpdateEntityCmd) Run(ctx *CLIContext) error {
	args := []string{c.FolderID}
	if c.Name != "" {
		args = append(args, "name="+c.Name)
	}
	if c.Description != "" {
		args = append(args, "description="+c.Description)
	}
	if c.ParentID != "" {
		args = append(args, "parent_id="+c.ParentID)
	}
	handleCDPUpdateEntityFolder(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPFoldersDeleteEntityCmd struct {
	FolderID string `kong:"arg,help='Folder ID'"`
}

func (c *CDPFoldersDeleteEntityCmd) Run(ctx *CLIContext) error {
	handleCDPDeleteEntityFolder(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	return nil
}

type CDPFoldersGetEntitiesCmd struct {
	FolderID string `kong:"arg,help='Folder ID'"`
}

func (c *CDPFoldersGetEntitiesCmd) Run(ctx *CLIContext) error {
	handleCDPGetEntitiesByFolder(ctx.Context, ctx.Client, []string{c.FolderID}, ctx.GlobalFlags)
	return nil
}

// CDP Tokens commands
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
	handleCDPListTokens(ctx.Context, ctx.Client, c, ctx.GlobalFlags)
	return nil
}

type CDPTokensGetEntityCmd struct {
	TokenID string `kong:"arg,help='Token ID'"`
}

func (c *CDPTokensGetEntityCmd) Run(ctx *CLIContext) error {
	handleCDPGetEntityToken(ctx.Context, ctx.Client, []string{c.TokenID}, ctx.GlobalFlags)
	return nil
}

type CDPTokensUpdateEntityCmd struct {
	TokenID string   `kong:"arg,help='Token ID'"`
	Updates []string `kong:"arg,help='Updates (key=value)'"`
}

func (c *CDPTokensUpdateEntityCmd) Run(ctx *CLIContext) error {
	args := append([]string{c.TokenID}, c.Updates...)
	handleCDPUpdateEntityToken(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPTokensDeleteEntityCmd struct {
	TokenID string `kong:"arg,help='Token ID'"`
}

func (c *CDPTokensDeleteEntityCmd) Run(ctx *CLIContext) error {
	handleCDPDeleteEntityToken(ctx.Context, ctx.Client, []string{c.TokenID}, ctx.GlobalFlags)
	return nil
}

// Workflow commands
type WorkflowCmd struct {
	List     WorkflowListCmd     `kong:"cmd,aliases='ls',help='List workflows'"`
	Get      WorkflowGetCmd      `kong:"cmd,aliases='show',help='Get workflow details'"`
	Create   WorkflowCreateCmd   `kong:"cmd,help='Create a new workflow'"`
	Update   WorkflowUpdateCmd   `kong:"cmd,help='Update workflow'"`
	Delete   WorkflowDeleteCmd   `kong:"cmd,aliases='rm',help='Delete workflow'"`
	Start    WorkflowStartCmd    `kong:"cmd,aliases='run',help='Start workflow execution'"`
	Attempts WorkflowAttemptsCmd `kong:"cmd,aliases='attempt',help='Workflow attempt management'"`
	Schedule WorkflowScheduleCmd `kong:"cmd,help='Workflow schedule management'"`
	Tasks    WorkflowTasksCmd    `kong:"cmd,aliases='task',help='Workflow task management'"`
	Logs     WorkflowLogsCmd     `kong:"cmd,aliases='log',help='Workflow log management'"`
	Projects WorkflowProjectsCmd `kong:"cmd,aliases='project,proj',help='Workflow project management'"`
}

type WorkflowListCmd struct{}

func (w *WorkflowListCmd) Run(ctx *CLIContext) error {
	handleWorkflowList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type WorkflowGetCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowGetCmd) Run(ctx *CLIContext) error {
	handleWorkflowGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowCreateCmd struct {
	Name    string `kong:"arg,help='Workflow name'"`
	Project string `kong:"arg,help='Project name'"`
	Config  string `kong:"arg,help='Workflow configuration (YAML)'"`
}

func (w *WorkflowCreateCmd) Run(ctx *CLIContext) error {
	handleWorkflowCreate(ctx.Context, ctx.Client, []string{w.Name, w.Project, w.Config}, ctx.GlobalFlags)
	return nil
}

type WorkflowUpdateCmd struct {
	WorkflowID int      `kong:"arg,help='Workflow ID'"`
	Updates    []string `kong:"arg,help='Updates (key=value)'"`
}

func (w *WorkflowUpdateCmd) Run(ctx *CLIContext) error {
	args := append([]string{fmt.Sprintf("%d", w.WorkflowID)}, w.Updates...)
	handleWorkflowUpdate(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type WorkflowDeleteCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowDeleteCmd) Run(ctx *CLIContext) error {
	handleWorkflowDelete(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, ctx.GlobalFlags)
	return nil
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
	handleWorkflowStart(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
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
	handleWorkflowAttemptList(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowAttemptsGetCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
	AttemptID  int `kong:"arg,help='Attempt ID'"`
}

func (w *WorkflowAttemptsGetCmd) Run(ctx *CLIContext) error {
	handleWorkflowAttemptGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowAttemptsKillCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
	AttemptID  int `kong:"arg,help='Attempt ID'"`
}

func (w *WorkflowAttemptsKillCmd) Run(ctx *CLIContext) error {
	handleWorkflowAttemptKill(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, ctx.GlobalFlags)
	return nil
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
	handleWorkflowAttemptRetry(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
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
	handleWorkflowScheduleGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowScheduleEnableCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowScheduleEnableCmd) Run(ctx *CLIContext) error {
	handleWorkflowScheduleEnable(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowScheduleDisableCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowScheduleDisableCmd) Run(ctx *CLIContext) error {
	handleWorkflowScheduleDisable(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowScheduleUpdateCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	Cron       string `kong:"arg,help='Cron expression'"`
	Timezone   string `kong:"arg,help='Timezone'"`
	Delay      int    `kong:"arg,help='Delay in seconds'"`
}

func (w *WorkflowScheduleUpdateCmd) Run(ctx *CLIContext) error {
	handleWorkflowScheduleUpdate(ctx.Context, ctx.Client, []string{
		fmt.Sprintf("%d", w.WorkflowID), w.Cron, w.Timezone, fmt.Sprintf("%d", w.Delay),
	}, ctx.GlobalFlags)
	return nil
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
	handleWorkflowTaskList(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowTasksGetCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	AttemptID  int    `kong:"arg,help='Attempt ID'"`
	TaskID     string `kong:"arg,help='Task ID'"`
}

func (w *WorkflowTasksGetCmd) Run(ctx *CLIContext) error {
	handleWorkflowTaskGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID), w.TaskID}, ctx.GlobalFlags)
	return nil
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
	handleWorkflowAttemptLog(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowLogsTaskCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	AttemptID  int    `kong:"arg,help='Attempt ID'"`
	TaskID     string `kong:"arg,help='Task ID'"`
}

func (w *WorkflowLogsTaskCmd) Run(ctx *CLIContext) error {
	handleWorkflowTaskLog(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID), w.TaskID}, ctx.GlobalFlags)
	return nil
}

type WorkflowProjectsCmd struct {
	List      WorkflowProjectsListCmd      `kong:"cmd,aliases='ls',help='List workflow projects'"`
	Get       WorkflowProjectsGetCmd       `kong:"cmd,aliases='show',help='Get project details'"`
	Create    WorkflowProjectsCreateCmd    `kong:"cmd,help='Create a new project'"`
	Push      WorkflowProjectsPushCmd      `kong:"cmd,help='Push project from directory (alias for create)'"`
	Workflows WorkflowProjectsWorkflowsCmd `kong:"cmd,aliases='wf',help='List workflows in project'"`
	Secrets   WorkflowProjectsSecretsCmd   `kong:"cmd,aliases='secret',help='Project secrets management'"`
}

type WorkflowProjectsListCmd struct{}

func (w *WorkflowProjectsListCmd) Run(ctx *CLIContext) error {
	handleWorkflowProjectList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	return nil
}

type WorkflowProjectsGetCmd struct {
	ProjectID int `kong:"arg,help='Project ID'"`
}

func (w *WorkflowProjectsGetCmd) Run(ctx *CLIContext) error {
	handleWorkflowProjectGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowProjectsCreateCmd struct {
	Name string `kong:"arg,help='Project name'"`
	Path string `kong:"arg,help='Directory path or archive file path'"`
}

func (w *WorkflowProjectsCreateCmd) Run(ctx *CLIContext) error {
	handleWorkflowProjectCreate(ctx.Context, ctx.Client, []string{w.Name, w.Path}, ctx.GlobalFlags)
	return nil
}

type WorkflowProjectsPushCmd struct {
	Name string `kong:"arg,help='Project name'"`
	Path string `kong:"arg,help='Directory path or archive file path'"`
}

func (w *WorkflowProjectsPushCmd) Run(ctx *CLIContext) error {
	handleWorkflowProjectCreate(ctx.Context, ctx.Client, []string{w.Name, w.Path}, ctx.GlobalFlags)
	return nil
}

type WorkflowProjectsWorkflowsCmd struct {
	ProjectID int `kong:"arg,help='Project ID'"`
}

func (w *WorkflowProjectsWorkflowsCmd) Run(ctx *CLIContext) error {
	handleWorkflowProjectWorkflows(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID)}, ctx.GlobalFlags)
	return nil
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
	handleWorkflowProjectSecretsList(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID)}, ctx.GlobalFlags)
	return nil
}

type WorkflowProjectsSecretsSetCmd struct {
	ProjectID int    `kong:"arg,help='Project ID'"`
	Key       string `kong:"arg,help='Secret key'"`
	Value     string `kong:"arg,help='Secret value'"`
}

func (w *WorkflowProjectsSecretsSetCmd) Run(ctx *CLIContext) error {
	handleWorkflowProjectSecretsSet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID), w.Key, w.Value}, ctx.GlobalFlags)
	return nil
}

type WorkflowProjectsSecretsDeleteCmd struct {
	ProjectID int    `kong:"arg,help='Project ID'"`
	Key       string `kong:"arg,help='Secret key'"`
}

func (w *WorkflowProjectsSecretsDeleteCmd) Run(ctx *CLIContext) error {
	handleWorkflowProjectSecretsDelete(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.ProjectID), w.Key}, ctx.GlobalFlags)
	return nil
}

// Convert Kong CLI to legacy Flags structure for compatibility
func (cli *CLI) ToFlags() Flags {
	return Flags{
		APIKey:      cli.APIKey,
		Region:      cli.Region,
		Format:      cli.Format,
		Output:      cli.Output,
		Verbose:     cli.Verbose,
		Database:    "",    // Will be set by individual commands
		Status:      "",    // Will be set by individual commands
		Priority:    0,     // Will be set by individual commands
		Limit:       0,     // Will be set by individual commands
		WithDetails: false, // Will be set by individual commands
	}
}
