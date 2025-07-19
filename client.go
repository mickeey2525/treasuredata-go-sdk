// Package treasuredata provides a Go client library for interacting with the Treasure Data REST API.
package treasuredata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/go-querystring/query"
)

const (
	// Default API endpoint
	defaultBaseURL = "https://api.treasuredata.com"

	// API version
	apiVersion = "v3"

	// Default timeout
	defaultTimeout = 30 * time.Second
)

// Regional endpoints
var RegionalEndpoints = map[string]string{
	"us":    "https://api.treasuredata.com",
	"eu":    "https://api.eu01.treasuredata.com",
	"tokyo": "https://api.treasuredata.co.jp",
	"ap02":  "https://api.ap02.treasuredata.com",
}

// CDP Regional endpoints
var CDPRegionalEndpoints = map[string]string{
	"us":    "https://api-cdp.us01.treasuredata.com",
	"eu":    "https://api-cdp.eu01.treasuredata.com",
	"tokyo": "https://api-cdp.treasuredata.co.jp",
	"ap02":  "https://api-cdp.ap02.treasuredata.com",
}

// Workflow Regional endpoints
var WorkflowRegionalEndpoints = map[string]string{
	"us":    "https://api-workflow.us01.treasuredata.com",
	"eu":    "https://api-workflow.eu01.treasuredata.com",
	"tokyo": "https://api-workflow.treasuredata.co.jp",
	"ap02":  "https://api-workflow.ap02.treasuredata.com",
}

// TDTime represents a time that can be unmarshaled from Treasure Data's timestamp format
type TDTime struct {
	time.Time
}

// UnmarshalJSON implements the json.Unmarshaler interface for TDTime
func (t *TDTime) UnmarshalJSON(data []byte) error {
	// First, try to unmarshal as a number (Unix timestamp)
	var timestamp int64
	if err := json.Unmarshal(data, &timestamp); err == nil {
		t.Time = time.Unix(timestamp, 0)
		return nil
	}

	// If not a number, try as a string
	var timeStr string
	if err := json.Unmarshal(data, &timeStr); err != nil {
		// Handle null values
		if string(data) == "null" {
			t.Time = time.Time{}
			return nil
		}
		return err
	}

	// Handle empty timestamps
	if timeStr == "" {
		t.Time = time.Time{} // Zero time
		return nil
	}

	// Try multiple time formats that Treasure Data API might return
	formats := []string{
		"2006-01-02 15:04:05 UTC",  // Original format: "2020-06-11 10:25:10 UTC"
		time.RFC3339,               // RFC3339 format: "2025-03-28T05:11:24Z"
		time.RFC3339Nano,           // RFC3339 with nanoseconds: "2024-04-26T00:05:42.783Z"
		"2006-01-02T15:04:05Z",     // Alternative RFC3339
		"2006-01-02T15:04:05.000Z", // RFC3339 with milliseconds
	}

	var parsedTime time.Time
	var err error

	for _, format := range formats {
		parsedTime, err = time.Parse(format, timeStr)
		if err == nil {
			t.Time = parsedTime
			return nil
		}
	}

	// If all formats fail, return the last error
	return err
}

// MarshalJSON implements the json.Marshaler interface for TDTime
func (t TDTime) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.Time.Format("2006-01-02 15:04:05 UTC") + `"`), nil
}

// Client represents a Treasure Data API client
type Client struct {
	// HTTP client for making requests
	httpClient *http.Client

	// Base URL for API requests
	BaseURL *url.URL

	// CDP API URL
	CDPURL *url.URL

	// Workflow API URL
	WorkflowURL *url.URL

	// API key for authentication
	APIKey string

	// User agent for API requests
	UserAgent string

	// Services for different API resources
	Databases   *DatabasesService
	Tables      *TablesService
	Jobs        *JobsService
	Queries     *QueriesService
	Results     *ResultsService
	Users       *UsersService
	Permissions *PermissionsService
	BulkImport  *BulkImportService
	CDP         *CDPService
	Workflow    *WorkflowService
}

// ClientOption is a function that configures a Client
type ClientOption func(*Client)

// WithHTTPClient sets a custom HTTP client
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithEndpoint sets a custom API endpoint
func WithEndpoint(endpoint string) ClientOption {
	return func(c *Client) {
		u, err := url.Parse(endpoint)
		if err == nil {
			c.BaseURL = u
		}
	}
}

// WithRegion sets the API endpoint based on region
func WithRegion(region string) ClientOption {
	return func(c *Client) {
		regionLower := strings.ToLower(region)
		if endpoint, ok := RegionalEndpoints[regionLower]; ok {
			u, _ := url.Parse(endpoint)
			c.BaseURL = u
		}
		if cdpEndpoint, ok := CDPRegionalEndpoints[regionLower]; ok {
			u, _ := url.Parse(cdpEndpoint)
			c.CDPURL = u
		}
		if workflowEndpoint, ok := WorkflowRegionalEndpoints[regionLower]; ok {
			u, _ := url.Parse(workflowEndpoint)
			c.WorkflowURL = u
		}
	}
}

// WithUserAgent sets a custom user agent
func WithUserAgent(ua string) ClientOption {
	return func(c *Client) {
		c.UserAgent = ua
	}
}

// NewClient creates a new Treasure Data API client
func NewClient(apiKey string, opts ...ClientOption) (*Client, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("API key is required")
	}

	baseURL, _ := url.Parse(defaultBaseURL)
	cdpURL, _ := url.Parse(CDPRegionalEndpoints["us"])
	workflowURL, _ := url.Parse(WorkflowRegionalEndpoints["us"])

	c := &Client{
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		BaseURL:     baseURL,
		CDPURL:      cdpURL,
		WorkflowURL: workflowURL,
		APIKey:      apiKey,
		UserAgent:   "treasuredata-go-sdk/1.0.0",
	}

	// Apply options
	for _, opt := range opts {
		opt(c)
	}

	// Initialize services
	c.Databases = &DatabasesService{client: c}
	c.Tables = &TablesService{client: c}
	c.Jobs = &JobsService{client: c}
	c.Queries = &QueriesService{client: c}
	c.Results = &ResultsService{client: c}
	c.Users = &UsersService{client: c}
	c.Permissions = &PermissionsService{client: c}
	c.BulkImport = &BulkImportService{client: c}
	c.CDP = &CDPService{client: c}
	c.Workflow = &WorkflowService{client: c}

	return c, nil
}

// NewRequest creates an API request
func (c *Client) NewRequest(method, urlStr string, body interface{}) (*http.Request, error) {
	u, err := c.BaseURL.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		err := enc.Encode(body)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set("Authorization", fmt.Sprintf("TD1 %s", c.APIKey))
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/json")

	return req, nil
}

// NewCDPRequest creates an API request for CDP endpoints
func (c *Client) NewCDPRequest(method, urlStr string, body interface{}) (*http.Request, error) {
	u, err := c.CDPURL.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		err := enc.Encode(body)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set("Authorization", fmt.Sprintf("TD1 %s", c.APIKey))
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/json")

	return req, nil
}

// NewWorkflowBinaryRequest creates a workflow API request for binary data
func (c *Client) NewWorkflowBinaryRequest(method, urlStr string, data []byte, contentType string) (*http.Request, error) {
	u, err := c.WorkflowURL.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var body io.Reader
	if data != nil {
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}

	if data != nil && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	req.Header.Set("Authorization", fmt.Sprintf("TD1 %s", c.APIKey))
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/json")

	return req, nil
}

// NewCDPJSONAPIRequest creates an API request for CDP JSON:API endpoints
func (c *Client) NewCDPJSONAPIRequest(method, urlStr string, body interface{}) (*http.Request, error) {
	u, err := c.CDPURL.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		err := enc.Encode(body)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/vnd.treasuredata.v1+json")
	}

	req.Header.Set("Authorization", fmt.Sprintf("TD1 %s", c.APIKey))
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/vnd.treasuredata.v1+json")

	return req, nil
}

// NewWorkflowRequest creates an API request for Workflow endpoints
func (c *Client) NewWorkflowRequest(method, urlStr string, body interface{}) (*http.Request, error) {
	u, err := c.WorkflowURL.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		err := enc.Encode(body)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set("Authorization", fmt.Sprintf("TD1 %s", c.APIKey))
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/json")

	return req, nil
}

// Do sends an API request and returns the API response
func (c *Client) Do(ctx context.Context, req *http.Request, v interface{}) (*http.Response, error) {
	req = req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	err = CheckResponse(resp)
	if err != nil {
		return resp, err
	}

	if v != nil && resp.StatusCode != http.StatusNoContent {
		if w, ok := v.(io.Writer); ok {
			io.Copy(w, resp.Body)
		} else {
			decErr := json.NewDecoder(resp.Body).Decode(v)
			if decErr == io.EOF {
				decErr = nil // ignore EOF errors caused by empty response body
			}
			if decErr != nil {
				err = decErr
			}
		}
	}

	return resp, err
}

// ErrorResponse represents an error response from the API
type ErrorResponse struct {
	Response *http.Response
	Message  string `json:"message"`
	ErrorMsg string `json:"error"`
	Text     string `json:"text"`
	Severity string `json:"severity"`
}

func (r *ErrorResponse) Error() string {
	return fmt.Sprintf("%v %v: %d %v",
		r.Response.Request.Method, r.Response.Request.URL,
		r.Response.StatusCode, r.Message)
}

// CheckResponse checks the API response for errors
func CheckResponse(r *http.Response) error {
	if c := r.StatusCode; 200 <= c && c <= 299 {
		return nil
	}

	errorResponse := &ErrorResponse{Response: r}
	data, err := io.ReadAll(r.Body)
	if err == nil && data != nil {
		json.Unmarshal(data, errorResponse)
	}

	return errorResponse
}

// ListOptions specifies optional parameters to various List methods
type ListOptions struct {
	From   int    `url:"from,omitempty"`
	To     int    `url:"to,omitempty"`
	Status string `url:"status,omitempty"`
}

// QueryOptions represents query execution options
type QueryOptions struct {
	Query      string `json:"query"`
	Type       string `json:"type,omitempty"`
	Priority   int    `json:"priority,omitempty"`
	RetryLimit int    `json:"retry_limit,omitempty"`
	Result     string `json:"result,omitempty"`
	DomainKey  string `json:"domain_key,omitempty"`
}

// addOptions adds the parameters in opt as URL query parameters to s
func addOptions(s string, opt interface{}) (string, error) {
	v, err := query.Values(opt)
	if err != nil {
		return s, err
	}

	u, err := url.Parse(s)
	if err != nil {
		return s, err
	}

	if u.RawQuery == "" {
		u.RawQuery = v.Encode()
	} else {
		u.RawQuery = u.RawQuery + "&" + v.Encode()
	}

	return u.String(), nil
}
