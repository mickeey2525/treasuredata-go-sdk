package treasuredata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/trinodb/trino-go-client/trino"
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
}

// TDTrinoClientConfig holds configuration for the Trino client
type TDTrinoClientConfig struct {
	APIKey     string
	Region     string
	Endpoint   string
	Database   string
	Source     string
	HTTPClient *http.Client
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

	// Wrap the HTTP client to add the X-Trino-User header
	wrappedClient := &http.Client{
		Timeout: httpClient.Timeout,
		Transport: &trinoTransport{
			base:   httpClient.Transport,
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

	client := &TDTrinoClient{
		db:       db,
		apiKey:   config.APIKey,
		region:   config.Region,
		endpoint: endpoint,
		database: config.Database,
		source:   config.Source,
	}

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
		return wrapError(c.db.Close())
	}
	return nil
}

// Ping verifies the connection to the database
func (c *TDTrinoClient) Ping(ctx context.Context) error {
	return wrapError(c.db.PingContext(ctx))
}

// Query executes a query and returns the rows
func (c *TDTrinoClient) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// Strip trailing semicolons - Trino doesn't expect them
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	rows, err := c.db.QueryContext(ctx, query, args...)
	return rows, wrapError(err)
}

// QueryRow executes a query that is expected to return at most one row
func (c *TDTrinoClient) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	// Strip trailing semicolons - Trino doesn't expect them
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	return c.db.QueryRowContext(ctx, query, args...)
}

// Exec executes a query without returning any rows
func (c *TDTrinoClient) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// Strip trailing semicolons - Trino doesn't expect them
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	result, err := c.db.ExecContext(ctx, query, args...)
	return result, wrapError(err)
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
