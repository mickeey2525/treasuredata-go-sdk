package treasuredata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/trinodb/trino-go-client/trino"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"sync"
)

const (
	defaultTrinoPort = 443
	defaultCatalog   = "td"
)

// TrinoRegionalEndpoints maps regions to their Trino endpoints
var TrinoRegionalEndpoints = map[string]string{
	"us":    "api-presto.treasuredata.com",
	"tokyo": "api-presto.treasuredata.co.jp",
	"eu":    "api-presto.eu01.treasuredata.com",
	"ap02":  "api-presto.ap02.treasuredata.com",
	"ap03":  "api-presto.ap03.treasuredata.com",
}

// TDTrinoClient represents a Treasure Data Trino client
type TDTrinoClient struct {
	db       *sql.DB
	apiKey   string
	region   string
	endpoint string
	database string
	source   string
	tracer   trace.Tracer
	meter    metric.Meter

	// Metrics instruments
	queryDuration   metric.Float64Histogram
	queryCounter    metric.Int64Counter
	connectionGauge metric.Int64UpDownCounter
	rowsProcessed   metric.Int64Counter
	bytesProcessed  metric.Int64Counter
}

// TDTrinoClientConfig holds configuration for the Trino client
type TDTrinoClientConfig struct {
	APIKey        string
	Region        string
	Endpoint      string
	Database      string
	Source        string
	HTTPClient    *http.Client
	EnableTracing bool
	Tracer        trace.Tracer
	Meter         metric.Meter
}

// TDTrinoError wraps errors to remove sensitive information
type TDTrinoError struct {
	Original error
	Message  string
}

func (e *TDTrinoError) Error() string {
	return e.Message
}

func (e *TDTrinoError) Unwrap() error {
	return e.Original
}

// trinoTransport wraps an http.RoundTripper to add the X-Trino-User header
type trinoTransport struct {
	base   http.RoundTripper
	apiKey string
}

// RoundTrip implements http.RoundTripper
func (t *trinoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	reqCopy := req.Clone(req.Context())
	reqCopy.Header.Set("X-Trino-User", t.apiKey)

	// Use the base transport or default
	transport := t.base
	if transport == nil {
		transport = http.DefaultTransport
	}

	return transport.RoundTrip(reqCopy)
}

// otelTrinoProgressUpdater captures Trino query progress to enrich spans
type otelTrinoProgressUpdater struct {
	span trace.Span
	once sync.Once
}

func (u *otelTrinoProgressUpdater) Update(info trino.QueryProgressInfo) {
	// Set query ID only once when available
	u.once.Do(func() {
		if u.span != nil && info.QueryId != "" {
			u.span.SetAttributes(attribute.String("trino.query_id", info.QueryId))
		}
	})
}

// wrapError removes sensitive information from errors
func wrapError(err error) error {
	if err == nil {
		return nil
	}

	// Remove API key from error messages
	msg := err.Error()
	if strings.Contains(msg, "/") {
		// API key format is account_id/api_key, remove everything after the first slash
		parts := strings.Split(msg, "/")
		if len(parts) > 1 {
			msg = parts[0] + "/[REDACTED]"
			for i := 2; i < len(parts); i++ {
				msg += "/" + parts[i]
			}
		}
	}

	return &TDTrinoError{
		Original: err,
		Message:  msg,
	}
}

// EscapeIdentifier escapes a SQL identifier
func EscapeIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// EscapeStringLiteral escapes a SQL string literal
func EscapeStringLiteral(literal string) string {
	return `'` + strings.ReplaceAll(literal, `'`, `''`) + `'`
}

// sanitizeSQL removes sensitive literals from SQL queries for tracing
func sanitizeSQL(query string) string {
	// Remove string literals
	re := regexp.MustCompile(`'[^']*'`)
	sanitized := re.ReplaceAllString(query, "'?'")

	// Remove numeric literals (but keep simple numbers like column references)
	re = regexp.MustCompile(`\b\d{4,}\b`) // Numbers with 4+ digits are likely sensitive
	sanitized = re.ReplaceAllString(sanitized, "?")

	// Limit length to prevent huge spans
	if len(sanitized) > 1000 {
		sanitized = sanitized[:1000] + "..."
	}

	return sanitized
}

// extractSQLOperation extracts the SQL operation type from a query
func extractSQLOperation(query string) string {
	query = strings.TrimSpace(strings.ToUpper(query))
	parts := strings.Fields(query)
	if len(parts) > 0 {
		return parts[0]
	}
	return "UNKNOWN"
}

// NewTDTrinoClient creates a new Treasure Data Trino client
func NewTDTrinoClient(config TDTrinoClientConfig) (*TDTrinoClient, error) {
	if config.APIKey == "" {
		if apiKey := os.Getenv("TD_API_KEY"); apiKey != "" {
			config.APIKey = apiKey
		} else {
			return nil, fmt.Errorf("API key is required (set TD_API_KEY environment variable or provide in config)")
		}
	}

	if config.Region == "" {
		config.Region = "us"
	}

	if config.Database == "" {
		config.Database = "sample_datasets"
	}

	if config.Source == "" {
		config.Source = "treasuredata-go-sdk"
	}

	// Determine endpoint
	endpoint := config.Endpoint
	if endpoint == "" {
		if regionEndpoint, ok := TrinoRegionalEndpoints[config.Region]; ok {
			endpoint = regionEndpoint
		} else {
			return nil, fmt.Errorf("unknown region: %s", config.Region)
		}
	}

	// Build DSN
	dsn := buildDSN(endpoint, config.Database, config.Source)

	// Create custom client with X-Trino-User header
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	// Build base transport and wrap with otelhttp for HTTP instrumentation
	baseTransport := httpClient.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	instrumentedBase := otelhttp.NewTransport(baseTransport)

	// Wrap the HTTP client to add the X-Trino-User header and keep instrumentation
	wrappedClient := &http.Client{
		Timeout: httpClient.Timeout,
		Transport: &trinoTransport{
			base:   instrumentedBase,
			apiKey: config.APIKey,
		},
	}

	// Register custom client
	clientName := fmt.Sprintf("td_%s_%d", config.Region, time.Now().UnixNano())
	trino.RegisterCustomClient(clientName, wrappedClient)

	// Build DSN with custom client
	dsnWithClient := fmt.Sprintf("%s&custom_client=%s", dsn, clientName)

	// Open database connection
	db, err := sql.Open("trino", dsnWithClient)
	if err != nil {
		return nil, wrapError(err)
	}

	// Initialize tracer
	var tracer trace.Tracer
	if config.EnableTracing && config.Tracer != nil {
		tracer = config.Tracer
	} else if config.EnableTracing {
		tracer = otel.Tracer("tdcli-trino")
	} else {
		tracer = otel.Tracer("tdcli-trino") // This will be a no-op tracer by default
	}

	// Initialize meter
	var meter metric.Meter
	if config.EnableTracing && config.Meter != nil {
		meter = config.Meter
	} else if config.EnableTracing {
		meter = otel.Meter("tdcli-trino")
	} else {
		meter = otel.Meter("tdcli-trino") // This will be a no-op meter by default
	}

	client := &TDTrinoClient{
		db:       db,
		apiKey:   config.APIKey,
		region:   config.Region,
		endpoint: endpoint,
		database: config.Database,
		source:   config.Source,
		tracer:   tracer,
		meter:    meter,
	}

	// Initialize metrics instruments
	if err := client.initializeMetrics(); err != nil {
		// Log error but don't fail client creation
		// This allows graceful degradation if metrics can't be initialized
		fmt.Printf("Warning: Failed to initialize Trino client metrics: %v\n", err)
	}

	// Record connection creation
	client.recordConnectionChange(context.Background(), 1)

	return client, nil
}

// NewTDTrinoClientWithHTTPClient creates a new client with a custom HTTP client
func NewTDTrinoClientWithHTTPClient(httpClient *http.Client) (*TDTrinoClient, error) {
	config := TDTrinoClientConfig{
		HTTPClient: httpClient,
	}
	return NewTDTrinoClient(config)
}

// buildDSN constructs the Trino DSN
func buildDSN(endpoint, database, source string) string {
	u := &url.URL{
		Scheme: "https",
		User:   url.User("td"), // Dummy user required by Trino protocol
		Host:   fmt.Sprintf("%s:%d", endpoint, defaultTrinoPort),
		Path:   "/",
	}

	params := url.Values{}
	params.Set("catalog", defaultCatalog)
	params.Set("schema", database)
	if source != "" {
		params.Set("source", source)
	}
	u.RawQuery = params.Encode()

	return u.String()
}

// DB returns the underlying sql.DB instance
func (c *TDTrinoClient) DB() *sql.DB {
	return c.db
}

// Close closes the database connection
func (c *TDTrinoClient) Close() error {
	if c.db != nil {
		// Record connection closure
		c.recordConnectionChange(context.Background(), -1)
		return wrapError(c.db.Close())
	}
	return nil
}

// Ping verifies the connection to the database
func (c *TDTrinoClient) Ping(ctx context.Context) error {
	// Record start time for metrics
	startTime := time.Now()

	// Create span for the ping operation
	ctx, span := c.tracer.Start(ctx, "trino.ping")
	defer span.End()

	// Add span attributes
	span.SetAttributes(
		attribute.String("db.system", "trino"),
		attribute.String("db.name", c.database),
		attribute.String("db.operation", "PING"),
		attribute.String("trino.catalog", defaultCatalog),
		attribute.String("trino.schema", c.database),
		attribute.String("trino.region", c.region),
		attribute.String("trino.endpoint", c.endpoint),
	)

	// Record metrics
	metricAttrs := metric.WithAttributes(
		attribute.String("database", c.database),
		attribute.String("operation", "PING"),
		attribute.String("region", c.region),
	)

	err := c.db.PingContext(ctx)

	// Record ping duration
	duration := time.Since(startTime).Seconds()
	if c.queryDuration != nil {
		c.queryDuration.Record(ctx, duration, metricAttrs)
	}

	// Record ping count
	if c.queryCounter != nil {
		successAttr := attribute.String("success", "true")
		if err != nil {
			successAttr = attribute.String("success", "false")
		}
		c.queryCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("database", c.database),
			attribute.String("operation", "PING"),
			attribute.String("region", c.region),
			successAttr,
		))
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return wrapError(err)
	}

	span.SetStatus(codes.Ok, "Ping successful")
	return nil
}

// Query executes a query and returns the rows
func (c *TDTrinoClient) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// Strip trailing semicolons - Trino doesn't expect them
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	// Record start time for metrics
	startTime := time.Now()
	operation := extractSQLOperation(query)

	// Create span for the query operation
	ctx, span := c.tracer.Start(ctx, "trino.query")
	defer span.End()

	// Add span attributes
	span.SetAttributes(
		attribute.String("db.system", "trino"),
		attribute.String("db.name", c.database),
		attribute.String("db.statement", sanitizeSQL(query)),
		attribute.String("db.operation", operation),
		attribute.String("trino.catalog", defaultCatalog),
		attribute.String("trino.schema", c.database),
		attribute.String("trino.region", c.region),
		attribute.String("trino.endpoint", c.endpoint),
	)

	// Record metrics
	metricAttrs := metric.WithAttributes(
		attribute.String("database", c.database),
		attribute.String("operation", operation),
		attribute.String("region", c.region),
	)

	// Add progress callback named args to capture query ID
	updater := &otelTrinoProgressUpdater{span: span}
	period := 500 * time.Millisecond
	argsWithProgress := append(args,
		sql.Named("X-Trino-Progress-Callback", updater),
		sql.Named("X-Trino-Progress-Callback-Period", period),
	)
	rows, err := c.db.QueryContext(ctx, query, argsWithProgress...)

	// Record query duration
	duration := time.Since(startTime).Seconds()
	if c.queryDuration != nil {
		c.queryDuration.Record(ctx, duration, metricAttrs)
	}

	// Record query count
	if c.queryCounter != nil {
		successAttr := attribute.String("success", "true")
		if err != nil {
			successAttr = attribute.String("success", "false")
		}
		c.queryCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("database", c.database),
			attribute.String("operation", operation),
			attribute.String("region", c.region),
			successAttr,
		))
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return rows, wrapError(err)
	}

	span.SetStatus(codes.Ok, "Query executed successfully")
	return rows, nil
}

// QueryRow executes a query that is expected to return at most one row
func (c *TDTrinoClient) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	// Strip trailing semicolons - Trino doesn't expect them
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	// Record start time for metrics
	startTime := time.Now()
	operation := extractSQLOperation(query)

	// Create span for the query row operation
	ctx, span := c.tracer.Start(ctx, "trino.query_row")
	defer span.End()

	// Add span attributes
	span.SetAttributes(
		attribute.String("db.system", "trino"),
		attribute.String("db.name", c.database),
		attribute.String("db.statement", sanitizeSQL(query)),
		attribute.String("db.operation", operation),
		attribute.String("trino.catalog", defaultCatalog),
		attribute.String("trino.schema", c.database),
		attribute.String("trino.region", c.region),
		attribute.String("trino.endpoint", c.endpoint),
	)

	// Record metrics
	metricAttrs := metric.WithAttributes(
		attribute.String("database", c.database),
		attribute.String("operation", operation),
		attribute.String("region", c.region),
	)

	// Add progress callback named args to capture query ID
	updater := &otelTrinoProgressUpdater{span: span}
	period := 500 * time.Millisecond
	argsWithProgress := append(args,
		sql.Named("X-Trino-Progress-Callback", updater),
		sql.Named("X-Trino-Progress-Callback-Period", period),
	)
	row := c.db.QueryRowContext(ctx, query, argsWithProgress...)

	// Record query duration
	duration := time.Since(startTime).Seconds()
	if c.queryDuration != nil {
		c.queryDuration.Record(ctx, duration, metricAttrs)
	}

	// Record query count (assume success since sql.Row doesn't expose errors until Scan)
	if c.queryCounter != nil {
		c.queryCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("database", c.database),
			attribute.String("operation", operation),
			attribute.String("region", c.region),
			attribute.String("success", "true"),
		))
	}

	// Note: sql.Row doesn't expose errors until Scan() is called,
	// so we can't check for errors here. The span will be marked as OK.
	span.SetStatus(codes.Ok, "QueryRow executed successfully")
	return row
}

// Exec executes a query without returning any rows
func (c *TDTrinoClient) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// Strip trailing semicolons - Trino doesn't expect them
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	// Record start time for metrics
	startTime := time.Now()
	operation := extractSQLOperation(query)

	// Create span for the exec operation
	ctx, span := c.tracer.Start(ctx, "trino.exec")
	defer span.End()

	// Add span attributes
	span.SetAttributes(
		attribute.String("db.system", "trino"),
		attribute.String("db.name", c.database),
		attribute.String("db.statement", sanitizeSQL(query)),
		attribute.String("db.operation", operation),
		attribute.String("trino.catalog", defaultCatalog),
		attribute.String("trino.schema", c.database),
		attribute.String("trino.region", c.region),
		attribute.String("trino.endpoint", c.endpoint),
	)

	// Record metrics
	metricAttrs := metric.WithAttributes(
		attribute.String("database", c.database),
		attribute.String("operation", operation),
		attribute.String("region", c.region),
	)

	// Add progress callback named args to capture query ID
	updater := &otelTrinoProgressUpdater{span: span}
	period := 500 * time.Millisecond
	argsWithProgress := append(args,
		sql.Named("X-Trino-Progress-Callback", updater),
		sql.Named("X-Trino-Progress-Callback-Period", period),
	)
	result, err := c.db.ExecContext(ctx, query, argsWithProgress...)

	// Record query duration
	duration := time.Since(startTime).Seconds()
	if c.queryDuration != nil {
		c.queryDuration.Record(ctx, duration, metricAttrs)
	}

	// Record query count
	if c.queryCounter != nil {
		successAttr := attribute.String("success", "true")
		if err != nil {
			successAttr = attribute.String("success", "false")
		}
		c.queryCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("database", c.database),
			attribute.String("operation", operation),
			attribute.String("region", c.region),
			successAttr,
		))
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return result, wrapError(err)
	}

	// Add result information if available and record rows processed
	if result != nil {
		if rowsAffected, err := result.RowsAffected(); err == nil {
			span.SetAttributes(attribute.Int64("db.rows_affected", rowsAffected))
			// Record rows processed metric
			if c.rowsProcessed != nil {
				c.rowsProcessed.Add(ctx, rowsAffected, metricAttrs)
			}
		}
	}

	span.SetStatus(codes.Ok, "Exec executed successfully")
	return result, nil
}

// Begin starts a transaction
func (c *TDTrinoClient) Begin(ctx context.Context) (*sql.Tx, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	return tx, wrapError(err)
}

// Prepare creates a prepared statement
func (c *TDTrinoClient) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	// Strip trailing semicolons - Trino doesn't expect them
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	stmt, err := c.db.PrepareContext(ctx, query)
	return stmt, wrapError(err)
}

// SetMaxOpenConns sets the maximum number of open connections to the database
func (c *TDTrinoClient) SetMaxOpenConns(n int) {
	c.db.SetMaxOpenConns(n)
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool
func (c *TDTrinoClient) SetMaxIdleConns(n int) {
	c.db.SetMaxIdleConns(n)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused
func (c *TDTrinoClient) SetConnMaxLifetime(d time.Duration) {
	c.db.SetConnMaxLifetime(d)
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle
func (c *TDTrinoClient) SetConnMaxIdleTime(d time.Duration) {
	c.db.SetConnMaxIdleTime(d)
}

// Stats returns database statistics
func (c *TDTrinoClient) Stats() sql.DBStats {
	return c.db.Stats()
}

// GetRegion returns the current region
func (c *TDTrinoClient) GetRegion() string {
	return c.region
}

// GetDatabase returns the current database
func (c *TDTrinoClient) GetDatabase() string {
	return c.database
}

// GetEndpoint returns the current endpoint
func (c *TDTrinoClient) GetEndpoint() string {
	return c.endpoint
}

// Driver returns the Trino driver
func (c *TDTrinoClient) Driver() driver.Driver {
	return c.db.Driver()
}

// NewTDTrinoClientWithTracing creates a new Trino client with tracing enabled
func NewTDTrinoClientWithTracing(config TDTrinoClientConfig) (*TDTrinoClient, error) {
	config.EnableTracing = true
	return NewTDTrinoClient(config)
}

// NewTDTrinoClientWithOTEL creates a new Trino client with OpenTelemetry support
func NewTDTrinoClientWithOTEL(config TDTrinoClientConfig, tracer trace.Tracer, meter metric.Meter) (*TDTrinoClient, error) {
	config.EnableTracing = true
	config.Tracer = tracer
	config.Meter = meter
	return NewTDTrinoClient(config)
}

// initializeMetrics creates and initializes metric instruments
func (c *TDTrinoClient) initializeMetrics() error {
	var err error

	// Query duration histogram
	c.queryDuration, err = c.meter.Float64Histogram(
		"trino_query_duration",
		metric.WithDescription("Duration of Trino queries in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("failed to create query duration histogram: %w", err)
	}

	// Query counter
	c.queryCounter, err = c.meter.Int64Counter(
		"trino_query_total",
		metric.WithDescription("Total number of Trino queries executed"),
	)
	if err != nil {
		return fmt.Errorf("failed to create query counter: %w", err)
	}

	// Connection gauge
	c.connectionGauge, err = c.meter.Int64UpDownCounter(
		"trino_connections",
		metric.WithDescription("Number of active Trino connections"),
	)
	if err != nil {
		return fmt.Errorf("failed to create connection gauge: %w", err)
	}

	// Rows processed counter
	c.rowsProcessed, err = c.meter.Int64Counter(
		"trino_rows_processed",
		metric.WithDescription("Total number of rows processed by Trino queries"),
	)
	if err != nil {
		return fmt.Errorf("failed to create rows processed counter: %w", err)
	}

	// Bytes processed counter
	c.bytesProcessed, err = c.meter.Int64Counter(
		"trino_bytes_processed",
		metric.WithDescription("Total number of bytes processed by Trino queries"),
	)
	if err != nil {
		return fmt.Errorf("failed to create bytes processed counter: %w", err)
	}

	return nil
}

// recordConnectionChange records connection status changes
func (c *TDTrinoClient) recordConnectionChange(ctx context.Context, delta int64) {
	if c.connectionGauge != nil {
		c.connectionGauge.Add(ctx, delta, metric.WithAttributes(
			attribute.String("database", c.database),
			attribute.String("region", c.region),
			attribute.String("endpoint", c.endpoint),
		))
	}
}
