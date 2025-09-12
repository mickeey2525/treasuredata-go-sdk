package main

import (
	"context"
	"fmt"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
	"github.com/mickeey2525/treasuredata-go-sdk/cmd/tdcli/workflow"
	"github.com/mickeey2525/treasuredata-go-sdk/otel"
)

// Global CLI structure
type CLI struct {
	// Global flags
	APIKey  string `kong:"help='Treasure Data API key (format: account_id/api_key)',env='TD_API_KEY'"`
	Region  string `kong:"help='API region (us, eu, tokyo, ap02)',default='us'"`
	Format  string `kong:"help='Output format (json, table, csv)',default='table',enum='json,table,csv'"`
	Output  string `kong:"help='Output to file'"`
	Verbose bool   `kong:"short='v',help='Verbose output'"`

	// SSL/TLS Options
	InsecureSkipVerify bool   `kong:"help='Skip TLS certificate verification',env='TD_INSECURE_SKIP_VERIFY'"`
	CertFile           string `kong:"help='Client certificate file path',env='TD_CERT_FILE'"`
	KeyFile            string `kong:"help='Client private key file path',env='TD_KEY_FILE'"`
	CAFile             string `kong:"help='Custom CA certificate file path',env='TD_CA_FILE'"`

	// OpenTelemetry Configuration
	OTELEnabled        bool              `kong:"help='Enable OpenTelemetry tracing and metrics',env='OTEL_ENABLED'"`
	OTELServiceName    string            `kong:"help='OTEL service name',env='OTEL_SERVICE_NAME',default='tdcli'"`
	OTELServiceVersion string            `kong:"help='OTEL service version',env='OTEL_SERVICE_VERSION'"`
	OTELTraceEndpoint  string            `kong:"help='OTEL trace endpoint URL',env='OTEL_EXPORTER_OTLP_TRACES_ENDPOINT'"`
	OTELMetricEndpoint string            `kong:"help='OTEL metric endpoint URL',env='OTEL_EXPORTER_OTLP_METRICS_ENDPOINT'"`
	OTELEndpoint       string            `kong:"help='OTEL generic endpoint URL (used if specific endpoints not set)',env='OTEL_EXPORTER_OTLP_ENDPOINT'"`
	OTELSamplingRate   float64           `kong:"help='OTEL sampling rate (0.0-1.0)',env='OTEL_SAMPLING_RATE',default='1.0'"`
	OTELHeaders        map[string]string `kong:"help='OTEL exporter headers (key=value,key2=value2)',env='OTEL_EXPORTER_OTLP_HEADERS'"`
	OTELInsecure       bool              `kong:"help='Use insecure OTEL connection',env='OTEL_EXPORTER_OTLP_INSECURE'"`
	OTELBatchTimeout   time.Duration     `kong:"help='OTEL batch timeout',env='OTEL_EXPORTER_OTLP_TIMEOUT',default='5s'"`
	OTELBatchSize      int               `kong:"help='OTEL batch size',env='OTEL_EXPORTER_OTLP_BATCH_SIZE',default='512'"`
	OTELResourceAttrs  map[string]string `kong:"help='OTEL resource attributes (key=value,key2=value2)',env='OTEL_RESOURCE_ATTRIBUTES'"`

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
	Trino     TrinoCmd     `kong:"cmd,help='Trino SQL client'"`
}

// Global variable to signal handlers to return errors instead of calling os.Exit
var captureHandlerErrors = false

// runHandlerWithErrorCapture wraps handler functions to capture their errors
func runHandlerWithErrorCapture(handlerFunc func()) (err error) {
	// Set capture mode
	originalCaptureMode := captureHandlerErrors
	captureHandlerErrors = true

	// Use defer to restore original mode and capture panics
	defer func() {
		captureHandlerErrors = originalCaptureMode
		if r := recover(); r != nil {
			// Convert panic to error
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("command failed: %v", r)
			}
		}
	}()

	// Run the handler function - it should now panic with errors instead of calling os.Exit
	handlerFunc()
	return nil
}

// runInstrumented wraps a handler with OTEL CLI instrumentation + error capture.
// Use this for commands that currently call handlers directly so we emit
// a high-level CLI span in addition to HTTP-level spans.
func runInstrumented(ctx *CLIContext, commandName string, args []string, handlerFunc func()) error {
	return InstrumentedRun(ctx, commandName, args, func(ctx *CLIContext) error {
		return runHandlerWithErrorCapture(handlerFunc)
	})
}

// Version command
type VersionCmd struct{}

func (v *VersionCmd) Run(ctx *CLIContext) error {
	return InstrumentedRun(ctx, "version", []string{}, func(ctx *CLIContext) error {
		fmt.Printf("tdcli version %s\n", version)
		fmt.Printf("commit: %s\n", commit)
		fmt.Printf("built: %s\n", date)
		return nil
	})
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
	return InstrumentedRun(ctx, "databases.list", []string{}, func(ctx *CLIContext) error {
		return runHandlerWithErrorCapture(func() {
			handleDatabaseList(ctx.Context, ctx.Client, ctx.GlobalFlags)
		})
	})
}

type DatabasesGetCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesGetCmd) Run(ctx *CLIContext) error {
	return InstrumentedRun(ctx, "databases.get", []string{d.Name}, func(ctx *CLIContext) error {
		return runHandlerWithErrorCapture(func() {
			handleDatabaseGet(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
		})
	})
}

type DatabasesCreateCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesCreateCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleDatabaseCreate(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	})
}

type DatabasesDeleteCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesDeleteCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleDatabaseDelete(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	})
}

type DatabasesUpdateCmd struct {
	Name string `kong:"arg,help='Database name'"`
}

func (d *DatabasesUpdateCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleDatabaseUpdate(ctx.Context, ctx.Client, []string{d.Name}, ctx.GlobalFlags)
	})
}

// Table commands
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
	return InstrumentedRun(ctx, "queries.submit", []string{q.Query}, func(ctx *CLIContext) error {
		return runHandlerWithErrorCapture(func() {
			// Set database in global flags for compatibility
			ctx.GlobalFlags.Database = q.Database
			ctx.GlobalFlags.Priority = q.Priority
			ctx.GlobalFlags.Engine = q.Engine
			handleQuerySubmit(ctx.Context, ctx.Client, []string{q.Query}, ctx.GlobalFlags)
		})
	})
}

type QueryStatusCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (q *QueryStatusCmd) Run(ctx *CLIContext) error {
	return InstrumentedRun(ctx, "queries.status", []string{q.JobID}, func(ctx *CLIContext) error {
		return runHandlerWithErrorCapture(func() {
			handleQueryStatus(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
		})
	})
}

type QueryResultCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
	Limit int    `kong:"help='Limit number of result rows'"`
}

func (q *QueryResultCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		ctx.GlobalFlags.Limit = q.Limit
		handleQueryResult(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
	})
}

type QueryListCmd struct {
	Status string `kong:"help='Filter by job status'"`
}

func (q *QueryListCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		ctx.GlobalFlags.Status = q.Status
		handleQueryList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type QueryCancelCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (q *QueryCancelCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleQueryCancel(ctx.Context, ctx.Client, []string{q.JobID}, ctx.GlobalFlags)
	})
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
	return InstrumentedRun(ctx, "jobs.list", []string{}, func(ctx *CLIContext) error {
		return runHandlerWithErrorCapture(func() {
			ctx.GlobalFlags.Status = j.Status
			handleJobList(ctx.Context, ctx.Client, ctx.GlobalFlags)
		})
	})
}

type JobsGetCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (j *JobsGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleJobGet(ctx.Context, ctx.Client, []string{j.JobID}, ctx.GlobalFlags)
	})
}

type JobsCancelCmd struct {
	JobID string `kong:"arg,help='Job ID'"`
}

func (j *JobsCancelCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleJobCancel(ctx.Context, ctx.Client, []string{j.JobID}, ctx.GlobalFlags)
	})
}

// User commands
type UsersCmd struct {
	List UsersListCmd `kong:"cmd,aliases='ls',help='List users'"`
	Get  UsersGetCmd  `kong:"cmd,aliases='show',help='Get user details'"`
}

type UsersListCmd struct{}

func (u *UsersListCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleUserList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type UsersGetCmd struct {
	UserID string `kong:"arg,help='User ID'"`
}

func (u *UsersGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleUserGet(ctx.Context, ctx.Client, []string{u.UserID}, ctx.GlobalFlags)
	})
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
	return runHandlerWithErrorCapture(func() {
		handlePolicyList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type PermsPoliciesGetCmd struct {
	PolicyID int `kong:"arg,help='Policy ID'"`
}

func (p *PermsPoliciesGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.PolicyID)}, ctx.GlobalFlags)
	})
}

type PermsPoliciesCreateCmd struct {
	Name        string `kong:"arg,help='Policy name'"`
	Description string `kong:"help='Policy description'"`
}

func (p *PermsPoliciesCreateCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		args := []string{p.Name}
		if p.Description != "" {
			args = append(args, p.Description)
		}
		handlePolicyCreate(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type PermsPoliciesDeleteCmd struct {
	PolicyID int `kong:"arg,help='Policy ID'"`
}

func (p *PermsPoliciesDeleteCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyDelete(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.PolicyID)}, ctx.GlobalFlags)
	})
}

type PermsGroupsCmd struct {
	List   PermsGroupsListCmd   `kong:"cmd,aliases='ls',help='List all policy groups'"`
	Get    PermsGroupsGetCmd    `kong:"cmd,aliases='show',help='Get policy group details'"`
	Create PermsGroupsCreateCmd `kong:"cmd,help='Create a new policy group'"`
	Delete PermsGroupsDeleteCmd `kong:"cmd,aliases='rm',help='Delete a policy group'"`
}

type PermsGroupsListCmd struct{}

func (p *PermsGroupsListCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGroupList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type PermsGroupsGetCmd struct {
	GroupID string `kong:"arg,help='Policy group ID'"`
}

func (p *PermsGroupsGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGroupGet(ctx.Context, ctx.Client, []string{p.GroupID}, ctx.GlobalFlags)
	})
}

type PermsGroupsCreateCmd struct {
	Name string `kong:"arg,help='Policy group name'"`
}

func (p *PermsGroupsCreateCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGroupCreate(ctx.Context, ctx.Client, []string{p.Name}, ctx.GlobalFlags)
	})
}

type PermsGroupsDeleteCmd struct {
	GroupID string `kong:"arg,help='Policy group ID'"`
}

func (p *PermsGroupsDeleteCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGroupDelete(ctx.Context, ctx.Client, []string{p.GroupID}, ctx.GlobalFlags)
	})
}

type PermsUsersCmd struct {
	List PermsUsersListCmd `kong:"cmd,aliases='ls',help='List access control users'"`
	Get  PermsUsersGetCmd  `kong:"cmd,aliases='show',help='Get user access control details'"`
}

type PermsUsersListCmd struct {
	WithDetails bool `kong:"help='Include user email and name details',default=true"`
}

func (p *PermsUsersListCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		ctx.GlobalFlags.WithDetails = p.WithDetails
		handleAccessControlUserList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type PermsUsersGetCmd struct {
	UserID int `kong:"arg,help='User ID'"`
}

func (p *PermsUsersGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleAccessControlUserGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.UserID)}, ctx.GlobalFlags)
	})
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
	return runHandlerWithErrorCapture(func() {
		ctx.GlobalFlags.Limit = r.Limit
		handleResultGet(ctx.Context, ctx.Client, []string{r.JobID}, ctx.GlobalFlags)
	})
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
	return runHandlerWithErrorCapture(func() {
		handleBulkImportList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type ImportGetCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportGet(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportCreateCmd struct {
	Session  string `kong:"arg,help='Session name'"`
	Database string `kong:"arg,help='Database name'"`
	Table    string `kong:"arg,help='Table name'"`
}

func (i *ImportCreateCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportCreate(ctx.Context, ctx.Client, []string{i.Session, i.Database, i.Table}, ctx.GlobalFlags)
	})
}

type ImportDeleteCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportDeleteCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportDelete(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportUploadCmd struct {
	Session  string `kong:"arg,help='Session name'"`
	PartName string `kong:"arg,help='Part name'"`
	FilePath string `kong:"arg,help='File path'"`
}

func (i *ImportUploadCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportUpload(ctx.Context, ctx.Client, []string{i.Session, i.PartName, i.FilePath}, ctx.GlobalFlags)
	})
}

type ImportCommitCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportCommitCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportCommit(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportPerformCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportPerformCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportPerform(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportFreezeCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportFreezeCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportFreeze(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportUnfreezeCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportUnfreezeCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportUnfreeze(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

type ImportPartsCmd struct {
	Session string `kong:"arg,help='Session name'"`
}

func (i *ImportPartsCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleBulkImportParts(ctx.Context, ctx.Client, []string{i.Session}, ctx.GlobalFlags)
	})
}

// Flags struct for compatibility with existing handlers
type Flags struct {
	APIKey             string
	Region             string
	Format             string
	Output             string
	Verbose            bool
	Database           string
	Status             string
	Priority           int
	Limit              int
	WithDetails        bool
	Engine             string
	InsecureSkipVerify bool
	CertFile           string
	KeyFile            string
	CAFile             string
}

// Context structure for command execution
type CLIContext struct {
	Context     context.Context
	Client      *td.Client
	GlobalFlags Flags
	OTELManager *otel.OTELManager
}

// CDP commands
type CDPCmd struct {
	Segments            CDPSegmentsCmd            `kong:"cmd,aliases='segment',help='CDP segment management'"`
	Audiences           CDPAudiencesCmd           `kong:"cmd,aliases='audience',help='CDP audience management'"`
	Activations         CDPActivationsCmd         `kong:"cmd,aliases='activation',help='CDP activation management'"`
	Folders             CDPFoldersCmd             `kong:"cmd,aliases='folder',help='CDP folder management'"`
	Tokens              CDPTokensCmd              `kong:"cmd,aliases='token',help='CDP token management'"`
	Journeys            CDPJourneysCmd            `kong:"cmd,aliases='journey',help='CDP journey management'"`
	ActivationTemplates CDPActivationTemplatesCmd `kong:"cmd,aliases='activation-template',help='CDP activation template management'"`
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
	return runHandlerWithErrorCapture(func() {
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
	return runHandlerWithErrorCapture(func() {
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
	return runHandlerWithErrorCapture(func() {
		handleCDPActivationListWithForce(ctx.Context, ctx.Client, ctx.GlobalFlags, c.Force)
	})
}

type CDPActivationsGetCmd struct {
	AudienceID   string `kong:"arg,help='Audience ID'"`
	SegmentID    string `kong:"arg,help='Segment ID'"`
	ActivationID string `kong:"arg,help='Activation ID'"`
}

func (c *CDPActivationsGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
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

// CDP Journey commands
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
	handleCDPJourneyDuplicate(ctx.Context, ctx.Client, []string{c.RequestFile}, ctx.GlobalFlags)
	return nil
}

type CDPJourneysPauseCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
}

func (c *CDPJourneysPauseCmd) Run(ctx *CLIContext) error {
	handleCDPJourneyPause(ctx.Context, ctx.Client, []string{c.JourneyID}, ctx.GlobalFlags)
	return nil
}

type CDPJourneysResumeCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
}

func (c *CDPJourneysResumeCmd) Run(ctx *CLIContext) error {
	handleCDPJourneyResume(ctx.Context, ctx.Client, []string{c.JourneyID}, ctx.GlobalFlags)
	return nil
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
	handleCDPJourneyStatistics(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPJourneysCustomersCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
	Limit     int    `kong:"flag,help='Limit number of results',default='100'"`
	Offset    int    `kong:"flag,help='Offset for pagination',default='0'"`
}

func (c *CDPJourneysCustomersCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID, fmt.Sprintf("--limit=%d", c.Limit), fmt.Sprintf("--offset=%d", c.Offset)}
	handleCDPJourneyCustomers(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
}

type CDPJourneysStageCustomersCmd struct {
	JourneyID string `kong:"arg,help='Journey ID'"`
	StageID   string `kong:"arg,help='Stage ID'"`
	Limit     int    `kong:"flag,help='Limit number of results',default='100'"`
	Offset    int    `kong:"flag,help='Offset for pagination',default='0'"`
}

func (c *CDPJourneysStageCustomersCmd) Run(ctx *CLIContext) error {
	args := []string{c.JourneyID, c.StageID, fmt.Sprintf("--limit=%d", c.Limit), fmt.Sprintf("--offset=%d", c.Offset)}
	handleCDPJourneyStageCustomers(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	return nil
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
	handleCDPJourneyActivationCreate(ctx.Context, ctx.Client, []string{c.JourneyID, c.RequestFile}, ctx.GlobalFlags)
	return nil
}

type CDPJourneyActivationsGetCmd struct {
	JourneyID        string `kong:"arg,help='Journey ID'"`
	ActivationStepID string `kong:"arg,help='Activation Step ID'"`
}

func (c *CDPJourneyActivationsGetCmd) Run(ctx *CLIContext) error {
	handleCDPJourneyActivationGet(ctx.Context, ctx.Client, []string{c.JourneyID, c.ActivationStepID}, ctx.GlobalFlags)
	return nil
}

type CDPJourneyActivationsUpdateCmd struct {
	JourneyID        string `kong:"arg,help='Journey ID'"`
	ActivationStepID string `kong:"arg,help='Activation Step ID'"`
	RequestFile      string `kong:"arg,help='JSON file with activation update data'"`
}

func (c *CDPJourneyActivationsUpdateCmd) Run(ctx *CLIContext) error {
	handleCDPJourneyActivationUpdate(ctx.Context, ctx.Client, []string{c.JourneyID, c.ActivationStepID, c.RequestFile}, ctx.GlobalFlags)
	return nil
}

// CDP Activation Template commands
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

// Workflow commands
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
	workflow.HandleWorkflowScheduleGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	return nil
}

type WorkflowScheduleEnableCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowScheduleEnableCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	workflow.HandleWorkflowScheduleEnable(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	return nil
}

type WorkflowScheduleDisableCmd struct {
	WorkflowID int `kong:"arg,help='Workflow ID'"`
}

func (w *WorkflowScheduleDisableCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	workflow.HandleWorkflowScheduleDisable(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID)}, flags)
	return nil
}

type WorkflowScheduleUpdateCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	Cron       string `kong:"arg,help='Cron expression'"`
	Timezone   string `kong:"arg,help='Timezone'"`
	Delay      int    `kong:"arg,help='Delay in seconds'"`
}

func (w *WorkflowScheduleUpdateCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	workflow.HandleWorkflowScheduleUpdate(ctx.Context, ctx.Client, []string{
		fmt.Sprintf("%d", w.WorkflowID), w.Cron, w.Timezone, fmt.Sprintf("%d", w.Delay),
	}, flags)
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
	flags := workflow.Flags(ctx.GlobalFlags)
	workflow.HandleWorkflowTaskList(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, flags)
	return nil
}

type WorkflowTasksGetCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	AttemptID  int    `kong:"arg,help='Attempt ID'"`
	TaskID     string `kong:"arg,help='Task ID'"`
}

func (w *WorkflowTasksGetCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	workflow.HandleWorkflowTaskGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID), w.TaskID}, flags)
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
	flags := workflow.Flags(ctx.GlobalFlags)
	workflow.HandleWorkflowAttemptLog(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID)}, flags)
	return nil
}

type WorkflowLogsTaskCmd struct {
	WorkflowID int    `kong:"arg,help='Workflow ID'"`
	AttemptID  int    `kong:"arg,help='Attempt ID'"`
	TaskID     string `kong:"arg,help='Task ID'"`
}

func (w *WorkflowLogsTaskCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	workflow.HandleWorkflowTaskLog(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", w.WorkflowID), fmt.Sprintf("%d", w.AttemptID), w.TaskID}, flags)
	return nil
}

type WorkflowInitCmd struct {
	ProjectName string `kong:"arg,help='Name of the new workflow project'"`
}

func (w *WorkflowInitCmd) Run(ctx *CLIContext) error {
	flags := workflow.Flags(ctx.GlobalFlags)
	workflow.HandleWorkflowInit(ctx.Context, []string{w.ProjectName}, flags)
	return nil
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

// Trino commands
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
	// Add page size to flags for non-interactive queries
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

// Convert Kong CLI to legacy Flags structure for compatibility
func (cli *CLI) ToFlags() Flags {
	return Flags{
		APIKey:             cli.APIKey,
		Region:             cli.Region,
		Format:             cli.Format,
		Output:             cli.Output,
		Verbose:            cli.Verbose,
		Database:           "",    // Will be set by individual commands
		Status:             "",    // Will be set by individual commands
		Priority:           0,     // Will be set by individual commands
		Limit:              0,     // Will be set by individual commands
		WithDetails:        false, // Will be set by individual commands
		InsecureSkipVerify: cli.InsecureSkipVerify,
		CertFile:           cli.CertFile,
		KeyFile:            cli.KeyFile,
		CAFile:             cli.CAFile,
	}
}

// ToOTELConfig converts CLI OTEL flags to OTELConfig
func (cli *CLI) ToOTELConfig() *otel.OTELConfig {
	config := otel.DefaultOTELConfig()

	config.Enabled = cli.OTELEnabled
	config.ServiceName = cli.OTELServiceName
	config.ServiceVersion = cli.OTELServiceVersion
	config.TraceEndpoint = cli.OTELTraceEndpoint
	config.MetricEndpoint = cli.OTELMetricEndpoint
	config.SamplingRate = cli.OTELSamplingRate
	config.Headers = cli.OTELHeaders
	config.Insecure = cli.OTELInsecure
	config.BatchTimeout = cli.OTELBatchTimeout
	config.BatchSize = cli.OTELBatchSize
	config.ResourceAttrs = cli.OTELResourceAttrs

	// Handle generic endpoint if specific endpoints not set
	if cli.OTELEndpoint != "" {
		if config.TraceEndpoint == "" {
			config.TraceEndpoint = cli.OTELEndpoint + "/v1/traces"
		}
		if config.MetricEndpoint == "" {
			config.MetricEndpoint = cli.OTELEndpoint + "/v1/metrics"
		}
	}

	// Apply sensible default for traces only (avoid defaulting metrics to prevent 404s with Jaeger)
	if config.TraceEndpoint == "" {
		config.TraceEndpoint = "http://localhost:4318/v1/traces"
	}

	return config
}
