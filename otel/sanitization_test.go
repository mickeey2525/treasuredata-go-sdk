package otel

import (
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestDataSanitizer_SanitizeSQL(t *testing.T) {
	sanitizer := NewDataSanitizer(DefaultSanitizationConfig())

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple select with string literal",
			input:    "SELECT * FROM users WHERE name = 'john'",
			expected: "SELECT * FROM users WHERE name = '?'",
		},
		{
			name:     "select with multiple string literals",
			input:    "SELECT * FROM users WHERE name = 'john' AND email = 'john@example.com'",
			expected: "SELECT * FROM users WHERE name = '?' AND email = '?'",
		},
		{
			name:     "select with numeric literals",
			input:    "SELECT * FROM users WHERE age = 25 AND score = 98.5",
			expected: "SELECT * FROM users WHERE age = ? AND score = ?",
		},
		{
			name:     "select with hex literals",
			input:    "SELECT * FROM data WHERE id = 0x1A2B3C",
			expected: "SELECT * FROM data WHERE id = 0x?",
		},
		{
			name:     "complex query with mixed literals",
			input:    "INSERT INTO users (name, age, email) VALUES ('Alice', 30, 'alice@test.com')",
			expected: "INSERT INTO users (name, age, email) VALUES ('?', ?, '?')",
		},
		{
			name:     "query with escaped quotes",
			input:    "SELECT * FROM users WHERE name = 'John\\'s Account'",
			expected: "SELECT * FROM users WHERE name = '?'",
		},
		{
			name:     "empty query",
			input:    "",
			expected: "",
		},
		{
			name:     "query without literals",
			input:    "SELECT * FROM users WHERE active = true",
			expected: "SELECT * FROM users WHERE active = true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.SanitizeSQL(tt.input)
			if result != tt.expected {
				t.Errorf("SanitizeSQL() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestDataSanitizer_SanitizeSQL_Disabled(t *testing.T) {
	config := DefaultSanitizationConfig()
	config.EnableSQLSanitization = false
	sanitizer := NewDataSanitizer(config)

	input := "SELECT * FROM users WHERE name = 'john'"
	result := sanitizer.SanitizeSQL(input)

	if result != input {
		t.Errorf("SanitizeSQL() with disabled sanitization = %q, want %q", result, input)
	}
}

func TestDataSanitizer_SanitizeURL(t *testing.T) {
	sanitizer := NewDataSanitizer(DefaultSanitizationConfig())

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with API key parameter",
			input:    "https://api.example.com/data?api_key=secret123&limit=10",
			expected: "https://api.example.com/data?api_key=%5BREDACTED%5D&limit=10",
		},
		{
			name:     "URL with token parameter",
			input:    "https://api.example.com/data?token=abc123&format=json",
			expected: "https://api.example.com/data?format=json&token=%5BREDACTED%5D",
		},
		{
			name:     "URL with multiple sensitive parameters",
			input:    "https://api.example.com/data?api_key=secret&password=pass123&user=john",
			expected: "https://api.example.com/data?api_key=%5BREDACTED%5D&password=%5BREDACTED%5D&user=john",
		},
		{
			name:     "URL with credentials in user info",
			input:    "https://user:pass@api.example.com/data",
			expected: "https://%5BREDACTED%5D:%5BREDACTED%5D@api.example.com/data",
		},
		{
			name:     "URL without sensitive data",
			input:    "https://api.example.com/data?limit=10&format=json",
			expected: "https://api.example.com/data?limit=10&format=json",
		},
		{
			name:     "empty URL",
			input:    "",
			expected: "",
		},
		{
			name:     "malformed URL with sensitive params",
			input:    "not-a-url?api_key=secret123",
			expected: "not-a-url?api_key=%5BREDACTED%5D",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.SanitizeURL(tt.input)
			if result != tt.expected {
				t.Errorf("SanitizeURL() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestDataSanitizer_SanitizeURL_CustomParams(t *testing.T) {
	config := DefaultSanitizationConfig()
	config.CustomSensitiveParams = []string{"custom_key", "private_token"}
	sanitizer := NewDataSanitizer(config)

	input := "https://api.example.com/data?custom_key=secret&private_token=token123&public=data"
	result := sanitizer.SanitizeURL(input)

	if !strings.Contains(result, "custom_key=%5BREDACTED%5D") {
		t.Errorf("SanitizeURL() should redact custom_key, got %q", result)
	}
	if !strings.Contains(result, "private_token=%5BREDACTED%5D") {
		t.Errorf("SanitizeURL() should redact private_token, got %q", result)
	}
	if !strings.Contains(result, "public=data") {
		t.Errorf("SanitizeURL() should preserve public param, got %q", result)
	}
}

func TestDataSanitizer_SanitizeHeaders(t *testing.T) {
	sanitizer := NewDataSanitizer(DefaultSanitizationConfig())

	tests := []struct {
		name     string
		input    map[string]string
		expected map[string]string
	}{
		{
			name: "headers with authorization",
			input: map[string]string{
				"Authorization": "Bearer token123",
				"Content-Type":  "application/json",
				"User-Agent":    "tdcli/1.0",
			},
			expected: map[string]string{
				"Authorization": "[REDACTED]",
				"Content-Type":  "application/json",
				"User-Agent":    "tdcli/1.0",
			},
		},
		{
			name: "headers with API key",
			input: map[string]string{
				"X-API-Key":    "secret123",
				"Content-Type": "application/json",
			},
			expected: map[string]string{
				"X-API-Key":    "[REDACTED]",
				"Content-Type": "application/json",
			},
		},
		{
			name: "headers without sensitive data",
			input: map[string]string{
				"Content-Type": "application/json",
				"User-Agent":   "tdcli/1.0",
			},
			expected: map[string]string{
				"Content-Type": "application/json",
				"User-Agent":   "tdcli/1.0",
			},
		},
		{
			name:     "empty headers",
			input:    map[string]string{},
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.SanitizeHeaders(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("SanitizeHeaders() returned %d headers, want %d", len(result), len(tt.expected))
			}

			for key, expectedValue := range tt.expected {
				if actualValue, exists := result[key]; !exists {
					t.Errorf("SanitizeHeaders() missing header %q", key)
				} else if actualValue != expectedValue {
					t.Errorf("SanitizeHeaders() header %q = %q, want %q", key, actualValue, expectedValue)
				}
			}
		})
	}
}

func TestDataSanitizer_SanitizeHeaders_CustomHeaders(t *testing.T) {
	config := DefaultSanitizationConfig()
	config.CustomSensitiveHeaders = []string{"X-Custom-Secret", "Private-Header"}
	sanitizer := NewDataSanitizer(config)

	input := map[string]string{
		"X-Custom-Secret": "secret123",
		"Private-Header":  "private456",
		"Public-Header":   "public789",
	}

	result := sanitizer.SanitizeHeaders(input)

	if result["X-Custom-Secret"] != "[REDACTED]" {
		t.Errorf("SanitizeHeaders() should redact X-Custom-Secret, got %q", result["X-Custom-Secret"])
	}
	if result["Private-Header"] != "[REDACTED]" {
		t.Errorf("SanitizeHeaders() should redact Private-Header, got %q", result["Private-Header"])
	}
	if result["Public-Header"] != "public789" {
		t.Errorf("SanitizeHeaders() should preserve Public-Header, got %q", result["Public-Header"])
	}
}

func TestDataSanitizer_SanitizeAttributes(t *testing.T) {
	config := DefaultSanitizationConfig()
	config.MaxAttributeValueLength = 20 // Short limit for testing
	config.MaxAttributeKeyLength = 15   // Short limit for testing
	sanitizer := NewDataSanitizer(config)

	tests := []struct {
		name     string
		input    []attribute.KeyValue
		validate func(t *testing.T, result []attribute.KeyValue)
	}{
		{
			name: "attribute value truncation",
			input: []attribute.KeyValue{
				attribute.String("short", "short value"),
				attribute.String("long", "this is a very long attribute value that should be truncated"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 2 {
					t.Errorf("Expected 2 attributes, got %d", len(result))
					return
				}

				shortValue := result[0].Value.AsString()
				if shortValue != "short value" {
					t.Errorf("Short value should not be truncated, got %q", shortValue)
				}

				longValue := result[1].Value.AsString()
				if !strings.HasSuffix(longValue, TruncationSuffix) {
					t.Errorf("Long value should be truncated with suffix, got %q", longValue)
				}
				if len(longValue) != 20 {
					t.Errorf("Truncated value should be exactly 20 chars, got %d", len(longValue))
				}
			},
		},
		{
			name: "attribute key truncation",
			input: []attribute.KeyValue{
				attribute.String("very_long_attribute_key_name", "value"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 1 {
					t.Errorf("Expected 1 attribute, got %d", len(result))
					return
				}

				key := string(result[0].Key)
				if !strings.HasSuffix(key, TruncationSuffix) {
					t.Errorf("Long key should be truncated with suffix, got %q", key)
				}
				if len(key) != 15 {
					t.Errorf("Truncated key should be exactly 15 chars, got %d", len(key))
				}
			},
		},
		{
			name: "SQL statement sanitization",
			input: []attribute.KeyValue{
				attribute.String("db.statement", "SELECT * FROM users WHERE name = 'john'"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 1 {
					t.Errorf("Expected 1 attribute, got %d", len(result))
					return
				}

				value := result[0].Value.AsString()
				// The value should be sanitized first, then truncated
				if !strings.Contains(value, "SELECT * FROM users WHERE name = '?'") && !strings.HasSuffix(value, TruncationSuffix) {
					t.Errorf("SQL statement should be sanitized or truncated, got %q", value)
				}
			},
		},
		{
			name: "URL sanitization",
			input: []attribute.KeyValue{
				attribute.String("http.url", "https://api.example.com/data?api_key=secret123"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 1 {
					t.Errorf("Expected 1 attribute, got %d", len(result))
					return
				}

				value := result[0].Value.AsString()
				if strings.Contains(value, "secret123") {
					t.Errorf("URL should be sanitized to remove api_key, got %q", value)
				}
				// The URL might be truncated, so check for either [REDACTED] or truncation suffix
				if !strings.Contains(value, "[REDACTED]") && !strings.Contains(value, "%5BREDACTED%5D") && !strings.HasSuffix(value, TruncationSuffix) {
					t.Errorf("URL should contain [REDACTED] placeholder or be truncated, got %q", value)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.SanitizeAttributes(tt.input)
			tt.validate(t, result)
		})
	}
}

func TestDataSanitizer_SanitizeError(t *testing.T) {
	sanitizer := NewDataSanitizer(DefaultSanitizationConfig())

	tests := []struct {
		name     string
		input    error
		validate func(t *testing.T, result error)
	}{
		{
			name:  "nil error",
			input: nil,
			validate: func(t *testing.T, result error) {
				if result != nil {
					t.Errorf("Expected nil error, got %v", result)
				}
			},
		},
		{
			name:  "error with URL containing API key",
			input: errors.New("failed to connect to https://api.example.com/data?api_key=secret123"),
			validate: func(t *testing.T, result error) {
				if result == nil {
					t.Errorf("Expected non-nil error")
					return
				}

				errMsg := result.Error()
				if strings.Contains(errMsg, "secret123") {
					t.Errorf("Error message should not contain API key, got %q", errMsg)
				}
				if !strings.Contains(errMsg, "[REDACTED]") {
					t.Errorf("Error message should contain [REDACTED] placeholder, got %q", errMsg)
				}
			},
		},
		{
			name:  "error with SQL containing string literal",
			input: errors.New("SQL error in query: SELECT * FROM users WHERE name = 'john'"),
			validate: func(t *testing.T, result error) {
				if result == nil {
					t.Errorf("Expected non-nil error")
					return
				}

				errMsg := result.Error()
				if strings.Contains(errMsg, "'john'") {
					t.Errorf("Error message should not contain SQL literal, got %q", errMsg)
				}
				if !strings.Contains(errMsg, "'[REDACTED]'") {
					t.Errorf("Error message should contain sanitized SQL literal, got %q", errMsg)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.SanitizeError(tt.input)
			tt.validate(t, result)
		})
	}
}

func TestDataSanitizer_CreateSanitizedAttribute(t *testing.T) {
	config := DefaultSanitizationConfig()
	config.MaxAttributeValueLength = 30 // Reasonable limit for testing
	sanitizer := NewDataSanitizer(config)

	attr := sanitizer.CreateSanitizedAttribute("test.key", "this is a very long attribute value that should be truncated")

	key := string(attr.Key)
	value := attr.Value.AsString()

	if key != "test.key" {
		t.Errorf("Key should be preserved, got %q", key)
	}

	if len(value) != 30 {
		t.Errorf("Value should be truncated to 30 chars, got %d chars: %q", len(value), value)
	}

	if !strings.HasSuffix(value, TruncationSuffix) {
		t.Errorf("Value should end with truncation suffix, got %q", value)
	}
}

func TestDataSanitizer_ValidateAttributeKey(t *testing.T) {
	config := DefaultSanitizationConfig()
	config.MaxAttributeKeyLength = 10
	sanitizer := NewDataSanitizer(config)

	tests := []struct {
		name      string
		key       string
		wantError bool
	}{
		{
			name:      "valid short key",
			key:       "short",
			wantError: false,
		},
		{
			name:      "valid max length key",
			key:       "1234567890", // exactly 10 chars
			wantError: false,
		},
		{
			name:      "too long key",
			key:       "12345678901", // 11 chars
			wantError: true,
		},
		{
			name:      "empty key",
			key:       "",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sanitizer.ValidateAttributeKey(tt.key)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateAttributeKey() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestDataSanitizer_ValidateAttributeValue(t *testing.T) {
	config := DefaultSanitizationConfig()
	config.MaxAttributeValueLength = 10
	sanitizer := NewDataSanitizer(config)

	tests := []struct {
		name      string
		value     string
		wantError bool
	}{
		{
			name:      "valid short value",
			value:     "short",
			wantError: false,
		},
		{
			name:      "valid max length value",
			value:     "1234567890", // exactly 10 chars
			wantError: false,
		},
		{
			name:      "too long value",
			value:     "12345678901", // 11 chars
			wantError: true,
		},
		{
			name:      "empty value",
			value:     "",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sanitizer.ValidateAttributeValue(tt.value)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateAttributeValue() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestDefaultSanitizationConfig(t *testing.T) {
	config := DefaultSanitizationConfig()

	if !config.EnableSQLSanitization {
		t.Error("SQL sanitization should be enabled by default")
	}
	if !config.EnableURLSanitization {
		t.Error("URL sanitization should be enabled by default")
	}
	if !config.EnableAttributeSizeLimiting {
		t.Error("Attribute size limiting should be enabled by default")
	}
	if config.MaxAttributeValueLength != MaxAttributeValueLength {
		t.Errorf("Max attribute value length should be %d, got %d", MaxAttributeValueLength, config.MaxAttributeValueLength)
	}
	if config.MaxAttributeKeyLength != MaxAttributeKeyLength {
		t.Errorf("Max attribute key length should be %d, got %d", MaxAttributeKeyLength, config.MaxAttributeKeyLength)
	}
}

func TestDataSanitizer_SanitizeAttributes_ContentSanitization(t *testing.T) {
	// Test content sanitization without size limits
	config := DefaultSanitizationConfig()
	config.EnableAttributeSizeLimiting = false
	sanitizer := NewDataSanitizer(config)

	tests := []struct {
		name     string
		input    []attribute.KeyValue
		validate func(t *testing.T, result []attribute.KeyValue)
	}{
		{
			name: "SQL statement sanitization without truncation",
			input: []attribute.KeyValue{
				attribute.String("db.statement", "SELECT * FROM users WHERE name = 'john'"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 1 {
					t.Errorf("Expected 1 attribute, got %d", len(result))
					return
				}

				value := result[0].Value.AsString()
				expected := "SELECT * FROM users WHERE name = '?'"
				if value != expected {
					t.Errorf("SQL statement should be sanitized, got %q, want %q", value, expected)
				}
			},
		},
		{
			name: "URL sanitization without truncation",
			input: []attribute.KeyValue{
				attribute.String("http.url", "https://api.example.com/data?api_key=secret123"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 1 {
					t.Errorf("Expected 1 attribute, got %d", len(result))
					return
				}

				value := result[0].Value.AsString()
				if strings.Contains(value, "secret123") {
					t.Errorf("URL should be sanitized to remove api_key, got %q", value)
				}
				if !strings.Contains(value, "[REDACTED]") && !strings.Contains(value, "%5BREDACTED%5D") {
					t.Errorf("URL should contain [REDACTED] placeholder, got %q", value)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.SanitizeAttributes(tt.input)
			tt.validate(t, result)
		})
	}
}

func TestNewDataSanitizer_NilConfig(t *testing.T) {
	sanitizer := NewDataSanitizer(nil)

	if sanitizer.config == nil {
		t.Error("Sanitizer should have default config when nil is passed")
	}

	// Test that it works with default config
	result := sanitizer.SanitizeSQL("SELECT * FROM users WHERE name = 'john'")
	expected := "SELECT * FROM users WHERE name = '?'"
	if result != expected {
		t.Errorf("Sanitizer with nil config should work, got %q, want %q", result, expected)
	}
}
