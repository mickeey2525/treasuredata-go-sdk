package main

import (
	"context"
	"fmt"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
	"github.com/mickeey2525/treasuredata-go-sdk/otel"
)

// CLI is the root Kong CLI structure.
type CLI struct {
	// Global flags
	APIKey  string `kong:"help='Treasure Data API key (format: account_id/api_key)',env='TD_API_KEY'"`
	Region  string `kong:"help='API region (us, eu, tokyo, ap02, ap03)',default='us'"`
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
	Version         VersionCmd         `kong:"cmd,help='Show version'"`
	Config          ConfigCmd          `kong:"cmd,help='Configuration management'"`
	Databases       DatabasesCmd       `kong:"cmd,aliases='db',help='Database management'"`
	Tables          TablesCmd          `kong:"cmd,aliases='table',help='Table management'"`
	Queries         QueriesCmd         `kong:"cmd,aliases='query,q',help='Query execution'"`
	Jobs            JobsCmd            `kong:"cmd,aliases='job',help='Job management'"`
	Users           UsersCmd           `kong:"cmd,aliases='user',help='User management'"`
	Perms           PermsCmd           `kong:"cmd,aliases='permissions,acl',help='Access control and permissions'"`
	Results         ResultsCmd         `kong:"cmd,aliases='result',help='Query results management'"`
	Import          ImportCmd          `kong:"cmd,aliases='bulk-import',help='Bulk data import'"`
	CDP             CDPCmd             `kong:"cmd,help='Customer Data Platform (CDP) management'"`
	Workflow        WorkflowCmd        `kong:"cmd,aliases='wf',help='Workflow management'"`
	Trino           TrinoCmd           `kong:"cmd,help='Trino SQL client'"`
	Postback        PostbackCmd        `kong:"cmd,help='Postback event ingestion'"`
	Stream          StreamCmd          `kong:"cmd,aliases='stream-import',help='Stream data import'"`
	Personalization PersonalizationCmd `kong:"cmd,aliases='p13n',help='Personalization API'"`
	LLM             LLMCmd             `kong:"cmd,help='LLM API management'"`
}

// Flags is a legacy compatibility struct used by handler functions.
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

// CLIContext holds the execution context for a command invocation.
type CLIContext struct {
	Context     context.Context
	Client      *td.Client
	GlobalFlags Flags
	OTELManager *otel.OTELManager
}

// runInstrumented wraps a handler with OTEL CLI instrumentation. Handlers
// return errors directly; instrumentation records the result on the span.
func runInstrumented(ctx *CLIContext, commandName string, args []string, handlerFunc func() error) error {
	return InstrumentedRun(ctx, commandName, args, func(ctx *CLIContext) error {
		return handlerFunc()
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

// ToFlags converts CLI global flags to the legacy Flags struct.
func (cli *CLI) ToFlags() Flags {
	return Flags{
		APIKey:             cli.APIKey,
		Region:             cli.Region,
		Format:             cli.Format,
		Output:             cli.Output,
		Verbose:            cli.Verbose,
		Database:           "",
		Status:             "",
		Priority:           0,
		Limit:              0,
		WithDetails:        false,
		InsecureSkipVerify: cli.InsecureSkipVerify,
		CertFile:           cli.CertFile,
		KeyFile:            cli.KeyFile,
		CAFile:             cli.CAFile,
	}
}

// ToOTELConfig converts CLI OTEL flags to an OTELConfig.
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

	if cli.OTELEndpoint != "" {
		if config.TraceEndpoint == "" {
			config.TraceEndpoint = cli.OTELEndpoint + "/v1/traces"
		}
		if config.MetricEndpoint == "" {
			config.MetricEndpoint = cli.OTELEndpoint + "/v1/metrics"
		}
	}

	if config.TraceEndpoint == "" {
		config.TraceEndpoint = "http://localhost:4318/v1/traces"
	}

	return config
}
