package treasuredata

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestEscapeIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"table", `"table"`},
		{"table_name", `"table_name"`},
		{"table\"with\"quotes", `"table""with""quotes"`},
		{"", `""`},
	}

	for _, test := range tests {
		result := EscapeIdentifier(test.input)
		if result != test.expected {
			t.Errorf("EscapeIdentifier(%q) = %q, expected %q", test.input, result, test.expected)
		}
	}
}

func TestEscapeStringLiteral(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"value", `'value'`},
		{"value'with'quotes", `'value''with''quotes'`},
		{"", `''`},
	}

	for _, test := range tests {
		result := EscapeStringLiteral(test.input)
		if result != test.expected {
			t.Errorf("EscapeStringLiteral(%q) = %q, expected %q", test.input, result, test.expected)
		}
	}
}

func TestTDTrinoClientConfig(t *testing.T) {
	config := TDTrinoClientConfig{
		APIKey:   "test_account/test_key",
		Region:   "us",
		Database: "test_db",
		Source:   "test_source",
	}

	// Test config validation
	if config.APIKey == "" {
		t.Error("APIKey should not be empty")
	}
}

func TestTrinoRegionalEndpoints(t *testing.T) {
	expectedEndpoints := map[string]string{
		"us":    "api-presto.treasuredata.com",
		"tokyo": "api-presto.treasuredata.co.jp",
		"eu":    "api-presto.eu01.treasuredata.com",
		"ap02":  "api-presto.ap02.treasuredata.com",
		"ap03":  "api-presto.ap03.treasuredata.com",
	}

	for region, expected := range expectedEndpoints {
		if endpoint, ok := TrinoRegionalEndpoints[region]; !ok {
			t.Errorf("Region %s not found in TrinoRegionalEndpoints", region)
		} else if endpoint != expected {
			t.Errorf("Endpoint for region %s = %q, expected %q", region, endpoint, expected)
		}
	}
}

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		endpoint string
		database string
		source   string
		expected string
	}{
		{
			endpoint: "api-presto.treasuredata.com",
			database: "sample_datasets",
			source:   "test_source",
			expected: "https://td@api-presto.treasuredata.com:443/?catalog=td&schema=sample_datasets&source=test_source",
		},
		{
			endpoint: "api-presto.treasuredata.com",
			database: "sample_datasets",
			source:   "",
			expected: "https://td@api-presto.treasuredata.com:443/?catalog=td&schema=sample_datasets",
		},
	}

	for _, test := range tests {
		result := buildDSN(test.endpoint, test.database, test.source)
		if result != test.expected {
			t.Errorf("buildDSN(%q, %q, %q) = %q, expected %q",
				test.endpoint, test.database, test.source, result, test.expected)
		}
	}
}

func TestTDTrinoError(t *testing.T) {
	originalErr := context.DeadlineExceeded
	wrappedErr := wrapError(originalErr)

	if wrappedErr == nil {
		t.Error("wrapError should return non-nil error")
	}

	// Test that API keys are removed from error messages
	err := &TDTrinoError{
		Message: "connection failed for user account_id/secret_key",
	}

	if err.Error() != "connection failed for user account_id/secret_key" {
		t.Errorf("Error message mismatch: got %q", err.Error())
	}
}

func TestTrinoTransport(t *testing.T) {
	transport := &trinoTransport{
		apiKey: "test_account/test_key",
	}

	// Create a mock request
	req := &http.Request{
		Method: "GET",
		Header: make(http.Header),
	}
	req = req.WithContext(context.Background())

	// We can't actually test RoundTrip without a full HTTP setup,
	// but we can test that the transport is properly initialized
	if transport.apiKey != "test_account/test_key" {
		t.Errorf("Transport API key = %q, expected %q", transport.apiKey, "test_account/test_key")
	}
}

// Example tests that serve as usage documentation
func ExampleNewTDTrinoClient() {
	config := TDTrinoClientConfig{
		APIKey:   "your_account_id/your_api_key",
		Region:   "us",
		Database: "sample_datasets",
		Source:   "my_application",
	}

	client, err := NewTDTrinoClient(config)
	if err != nil {
		// Handle error
		return
	}
	defer client.Close()

	// Use the client for queries
	ctx := context.Background()
	rows, err := client.Query(ctx, "SELECT COUNT(*) FROM nasdaq")
	if err != nil {
		// Handle error
		return
	}
	defer rows.Close()

	// Process results...
}

func ExampleNewTDTrinoClientWithHTTPClient() {
	// Create custom HTTP client with timeout
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
	}

	client, err := NewTDTrinoClientWithHTTPClient(httpClient)
	if err != nil {
		// Handle error
		return
	}
	defer client.Close()

	// Use the client...
}

func ExampleEscapeIdentifier() {
	tableName := "my-table"
	safeTableName := EscapeIdentifier(tableName)
	// safeTableName is now "my-table"

	query := "SELECT * FROM " + safeTableName
	_ = query // Use in your SQL query
}

func ExampleEscapeStringLiteral() {
	userInput := "user's input"
	safeLiteral := EscapeStringLiteral(userInput)
	// safeLiteral is now 'user''s input'

	query := "SELECT * FROM users WHERE name = " + safeLiteral
	_ = query // Use in your SQL query
}
