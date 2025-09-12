package treasuredata

import (
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestTDTrinoClientOTELMetrics(t *testing.T) {
	// Test that OTEL metrics can be initialized
	config := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		EnableTracing: true,
		Tracer:        otel.Tracer("test"),
		Meter:         otel.Meter("test"),
	}

	client, err := NewTDTrinoClient(config)
	if err != nil {
		t.Fatalf("Failed to create Trino client with OTEL: %v", err)
	}
	defer client.Close()

	// Verify that the client has the OTEL components
	if client.tracer == nil {
		t.Error("Expected tracer to be set")
	}
	if client.meter == nil {
		t.Error("Expected meter to be set")
	}

	// Verify that metrics instruments are initialized
	if client.queryDuration == nil {
		t.Error("Expected queryDuration metric to be initialized")
	}
	if client.queryCounter == nil {
		t.Error("Expected queryCounter metric to be initialized")
	}
	if client.connectionGauge == nil {
		t.Error("Expected connectionGauge metric to be initialized")
	}
	if client.rowsProcessed == nil {
		t.Error("Expected rowsProcessed metric to be initialized")
	}
	if client.bytesProcessed == nil {
		t.Error("Expected bytesProcessed metric to be initialized")
	}
}

func TestNewTDTrinoClientWithOTEL(t *testing.T) {
	// Test the new convenience function
	config := TDTrinoClientConfig{
		APIKey: "test_account/test_key",
		Region: "us",
	}

	tracer := otel.Tracer("test")
	meter := otel.Meter("test")

	client, err := NewTDTrinoClientWithOTEL(config, tracer, meter)
	if err != nil {
		t.Fatalf("Failed to create Trino client with OTEL: %v", err)
	}
	defer client.Close()

	// Verify that the client has the OTEL components
	if client.tracer != tracer {
		t.Error("Expected tracer to match provided tracer")
	}
	if client.meter != meter {
		t.Error("Expected meter to match provided meter")
	}
}

func TestTDTrinoClientSQLSanitization(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple query",
			input:    "SELECT * FROM table",
			expected: "SELECT * FROM table",
		},
		{
			name:     "query with string literal",
			input:    "SELECT * FROM table WHERE name = 'sensitive_data'",
			expected: "SELECT * FROM table WHERE name = '?'",
		},
		{
			name:     "query with large number",
			input:    "SELECT * FROM table WHERE id = 123456789",
			expected: "SELECT * FROM table WHERE id = ?",
		},
		{
			name:     "query with small number",
			input:    "SELECT * FROM table WHERE status = 1",
			expected: "SELECT * FROM table WHERE status = 1",
		},
		{
			name:     "very long query",
			input:    "SELECT " + string(make([]byte, 1100)) + " FROM table",
			expected: "", // We'll check this separately
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeSQL(tt.input)
			if len(result) > 1003 { // Allow for "..." suffix
				t.Errorf("sanitizeSQL() result too long: %d characters", len(result))
			}
			if tt.name == "very long query" {
				if !strings.HasSuffix(result, "...") {
					t.Error("Expected long query to be truncated with '...'")
				}
				if len(result) != 1003 { // 1000 + "..."
					t.Errorf("Expected truncated query to be exactly 1003 characters, got %d", len(result))
				}
			} else if tt.expected != "" && result != tt.expected {
				t.Errorf("sanitizeSQL() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestTDTrinoClientSQLOperationExtraction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "SELECT query",
			input:    "SELECT * FROM table",
			expected: "SELECT",
		},
		{
			name:     "INSERT query",
			input:    "INSERT INTO table VALUES (1, 2, 3)",
			expected: "INSERT",
		},
		{
			name:     "UPDATE query",
			input:    "UPDATE table SET col = 'value'",
			expected: "UPDATE",
		},
		{
			name:     "DELETE query",
			input:    "DELETE FROM table WHERE id = 1",
			expected: "DELETE",
		},
		{
			name:     "lowercase query",
			input:    "select * from table",
			expected: "SELECT",
		},
		{
			name:     "query with leading whitespace",
			input:    "  \t\n SELECT * FROM table",
			expected: "SELECT",
		},
		{
			name:     "empty query",
			input:    "",
			expected: "UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractSQLOperation(tt.input)
			if result != tt.expected {
				t.Errorf("extractSQLOperation() = %v, want %v", result, tt.expected)
			}
		})
	}
}
