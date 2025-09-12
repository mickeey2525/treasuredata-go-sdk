package otel

import (
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestSecureCredentialHandler_ValidateAndSanitizeEndpoint(t *testing.T) {
	tests := []struct {
		name          string
		endpoint      string
		allowInsecure bool
		expectError   bool
		expectWarning bool
	}{
		{
			name:          "HTTPS endpoint",
			endpoint:      "https://api.example.com/v1/traces",
			allowInsecure: false,
			expectError:   false,
		},
		{
			name:          "HTTP localhost endpoint",
			endpoint:      "http://localhost:4317/v1/traces",
			allowInsecure: false,
			expectError:   false,
			expectWarning: true,
		},
		{
			name:          "HTTP remote endpoint with insecure allowed",
			endpoint:      "http://remote.example.com/v1/traces",
			allowInsecure: true,
			expectError:   false,
		},
		{
			name:          "HTTP remote endpoint with insecure not allowed",
			endpoint:      "http://remote.example.com/v1/traces",
			allowInsecure: false,
			expectError:   true,
		},
		{
			name:          "invalid endpoint",
			endpoint:      "://invalid-url",
			allowInsecure: false,
			expectError:   true,
		},
		{
			name:          "empty endpoint",
			endpoint:      "",
			allowInsecure: false,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSecurityConfig()
			config.AllowInsecureEndpoints = tt.allowInsecure
			handler := NewSecureCredentialHandler(config, nil)

			result, err := handler.ValidateAndSanitizeEndpoint(tt.endpoint)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && tt.endpoint != "" && result == "" {
				t.Errorf("Expected sanitized result but got empty string")
			}
		})
	}
}

func TestSecureCredentialHandler_SanitizeSpanAttributes(t *testing.T) {
	config := DefaultSecurityConfig()
	config.StrictCredentialFiltering = true
	handler := NewSecureCredentialHandler(config, DefaultSanitizationConfig())

	tests := []struct {
		name     string
		input    []attribute.KeyValue
		validate func(t *testing.T, result []attribute.KeyValue)
	}{
		{
			name: "redact sensitive attributes",
			input: []attribute.KeyValue{
				attribute.String("api_key", "secret123"),
				attribute.String("password", "mypassword"),
				attribute.String("user.name", "john"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 3 {
					t.Errorf("Expected 3 attributes, got %d", len(result))
					return
				}

				// Check that sensitive attributes are redacted
				for _, attr := range result {
					key := string(attr.Key)
					value := attr.Value.AsString()

					if key == "api_key" && value != "[REDACTED]" {
						t.Errorf("api_key should be redacted, got %q", value)
					}
					if key == "password" && value != "[REDACTED]" {
						t.Errorf("password should be redacted, got %q", value)
					}
					if key == "user.name" && value == "[REDACTED]" {
						t.Errorf("user.name should not be completely redacted, got %q", value)
					}
				}
			},
		},
		{
			name: "sanitize SQL statements",
			input: []attribute.KeyValue{
				attribute.String("db.statement", "SELECT * FROM users WHERE name = 'john'"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 1 {
					t.Errorf("Expected 1 attribute, got %d", len(result))
					return
				}

				value := result[0].Value.AsString()
				if strings.Contains(value, "'john'") {
					t.Errorf("SQL statement should be sanitized, got %q", value)
				}
			},
		},
		{
			name: "sanitize URLs",
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
					t.Errorf("URL should be sanitized, got %q", value)
				}
			},
		},
		{
			name: "sanitize email addresses",
			input: []attribute.KeyValue{
				attribute.String("user.email", "john.doe@example.com"),
			},
			validate: func(t *testing.T, result []attribute.KeyValue) {
				if len(result) != 1 {
					t.Errorf("Expected 1 attribute, got %d", len(result))
					return
				}

				value := result[0].Value.AsString()
				if value == "john.doe@example.com" {
					t.Errorf("Email should be partially sanitized, got %q", value)
				}
				if !strings.Contains(value, "@example.com") {
					t.Errorf("Email domain should be preserved, got %q", value)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.SanitizeSpanAttributes(tt.input)
			tt.validate(t, result)
		})
	}
}

func TestSecureCredentialHandler_SanitizeEmail(t *testing.T) {
	handler := NewSecureCredentialHandler(nil, nil)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "normal email",
			input:    "john.doe@example.com",
			expected: "j***e@example.com",
		},
		{
			name:     "short email",
			input:    "ab@example.com",
			expected: "[REDACTED]@example.com",
		},
		{
			name:     "medium email",
			input:    "john@example.com",
			expected: "j*@example.com",
		},
		{
			name:     "not an email",
			input:    "not-an-email",
			expected: "not-an-email",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.sanitizeEmail(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeEmail() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestSecureCredentialHandler_SanitizeIdentifier(t *testing.T) {
	handler := NewSecureCredentialHandler(nil, nil)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "UUID",
			input:    "550e8400-e29b-41d4-a716-446655440000",
			expected: "550e****0000",
		},
		{
			name:     "long ID",
			input:    "abcdefghijklmnop",
			expected: "abcd****mnop",
		},
		{
			name:     "medium ID",
			input:    "abcdefghij",
			expected: "ab****ij",
		},
		{
			name:     "short ID",
			input:    "abc123",
			expected: "abc123", // Not sanitized
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.sanitizeIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeIdentifier() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestSecureCredentialHandler_SanitizeError(t *testing.T) {
	handler := NewSecureCredentialHandler(nil, DefaultSanitizationConfig())

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
			name:  "error with token",
			input: errors.New("authentication failed: token=abc123xyz"),
			validate: func(t *testing.T, result error) {
				if result == nil {
					t.Errorf("Expected non-nil error")
					return
				}

				errMsg := result.Error()
				if strings.Contains(errMsg, "abc123xyz") {
					t.Errorf("Error should not contain token value, got %q", errMsg)
				}
				if !strings.Contains(errMsg, "[REDACTED]") {
					t.Errorf("Error should contain [REDACTED] placeholder, got %q", errMsg)
				}
			},
		},
		{
			name:  "error with API key",
			input: errors.New("request failed: key=secret123"),
			validate: func(t *testing.T, result error) {
				if result == nil {
					t.Errorf("Expected non-nil error")
					return
				}

				errMsg := result.Error()
				if strings.Contains(errMsg, "secret123") {
					t.Errorf("Error should not contain key value, got %q", errMsg)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.SanitizeError(tt.input)
			tt.validate(t, result)
		})
	}
}

func TestSecureCredentialHandler_ValidateConfiguration(t *testing.T) {
	handler := NewSecureCredentialHandler(DefaultSecurityConfig(), nil)

	tests := []struct {
		name           string
		config         *OTELConfig
		expectWarnings int
	}{
		{
			name: "secure configuration",
			config: &OTELConfig{
				TraceEndpoint:  "https://api.example.com/v1/traces",
				MetricEndpoint: "https://api.example.com/v1/metrics",
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			},
			expectWarnings: 0,
		},
		{
			name: "insecure endpoints",
			config: &OTELConfig{
				TraceEndpoint:  "http://remote.example.com/v1/traces",
				MetricEndpoint: "http://remote.example.com/v1/metrics",
			},
			expectWarnings: 2,
		},
		{
			name: "sensitive headers",
			config: &OTELConfig{
				TraceEndpoint: "https://api.example.com/v1/traces",
				Headers: map[string]string{
					"X-API-Password": "secret123",
					"X-Secret-Key":   "anothersecret",
				},
			},
			expectWarnings: 2,
		},
		{
			name: "localhost endpoints",
			config: &OTELConfig{
				TraceEndpoint:  "http://localhost:4317/v1/traces",
				MetricEndpoint: "http://127.0.0.1:4318/v1/metrics",
			},
			expectWarnings: 0, // Localhost is allowed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warnings := handler.ValidateConfiguration(tt.config)
			if len(warnings) != tt.expectWarnings {
				t.Errorf("Expected %d warnings, got %d: %v", tt.expectWarnings, len(warnings), warnings)
			}
		})
	}
}

func TestSecureCredentialHandler_ShouldRedactAttribute(t *testing.T) {
	config := DefaultSecurityConfig()
	config.StrictCredentialFiltering = true
	handler := NewSecureCredentialHandler(config, nil)

	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{
			name:     "api_key should be redacted",
			key:      "api_key",
			expected: true,
		},
		{
			name:     "password should be redacted",
			key:      "password",
			expected: true,
		},
		{
			name:     "authorization should be redacted",
			key:      "authorization",
			expected: true,
		},
		{
			name:     "user.name should not be redacted",
			key:      "user.name",
			expected: false,
		},
		{
			name:     "http.method should not be redacted",
			key:      "http.method",
			expected: false,
		},
		{
			name:     "bearer_token should be redacted",
			key:      "bearer_token",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.shouldRedactAttribute(tt.key)
			if result != tt.expected {
				t.Errorf("shouldRedactAttribute(%q) = %v, want %v", tt.key, result, tt.expected)
			}
		})
	}
}

func TestSecureCredentialHandler_IsPotentiallySensitive(t *testing.T) {
	handler := NewSecureCredentialHandler(nil, nil)

	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{
			name:     "user.email is sensitive",
			key:      "user.email",
			expected: true,
		},
		{
			name:     "user.id is sensitive",
			key:      "user.id",
			expected: true,
		},
		{
			name:     "http.url is sensitive",
			key:      "http.url",
			expected: true,
		},
		{
			name:     "db.statement is sensitive",
			key:      "db.statement",
			expected: true,
		},
		{
			name:     "http.method is not sensitive",
			key:      "http.method",
			expected: false,
		},
		{
			name:     "service.name is not sensitive",
			key:      "service.name",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.isPotentiallySensitive(tt.key)
			if result != tt.expected {
				t.Errorf("isPotentiallySensitive(%q) = %v, want %v", tt.key, result, tt.expected)
			}
		})
	}
}

func TestDefaultSecurityConfig(t *testing.T) {
	config := DefaultSecurityConfig()

	if config.AllowInsecureEndpoints {
		t.Error("AllowInsecureEndpoints should be false by default")
	}
	if !config.LogSecurityWarnings {
		t.Error("LogSecurityWarnings should be true by default")
	}
	if !config.StrictCredentialFiltering {
		t.Error("StrictCredentialFiltering should be true by default")
	}
	if config.MaxSecureAttributeLength != 256 {
		t.Errorf("MaxSecureAttributeLength should be 256, got %d", config.MaxSecureAttributeLength)
	}
}

func TestNewSecureCredentialHandler_NilConfigs(t *testing.T) {
	handler := NewSecureCredentialHandler(nil, nil)

	if handler.config == nil {
		t.Error("Handler should have default security config when nil is passed")
	}
	if handler.sanitizer == nil {
		t.Error("Handler should have sanitizer when nil config is passed")
	}

	// Test that it works with default configs
	attrs := []attribute.KeyValue{
		attribute.String("api_key", "secret123"),
	}
	result := handler.SanitizeSpanAttributes(attrs)
	if len(result) != 1 {
		t.Errorf("Expected 1 attribute, got %d", len(result))
	}
}
