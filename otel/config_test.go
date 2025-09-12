package otel

import (
	"os"
	"testing"
	"time"
)

func TestOTELConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *OTELConfig
		wantErr bool
	}{
		{
			name:    "disabled config is valid",
			config:  &OTELConfig{Enabled: false},
			wantErr: false,
		},
		{
			name: "valid enabled config",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test-service",
				SamplingRate:  0.5,
				BatchTimeout:  time.Second,
				BatchSize:     100,
				ExportTimeout: 30 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "empty service name",
			config: &OTELConfig{
				Enabled:     true,
				ServiceName: "",
			},
			wantErr: true,
		},
		{
			name: "invalid sampling rate - negative",
			config: &OTELConfig{
				Enabled:      true,
				ServiceName:  "test",
				SamplingRate: -0.1,
			},
			wantErr: true,
		},
		{
			name: "invalid sampling rate - too high",
			config: &OTELConfig{
				Enabled:      true,
				ServiceName:  "test",
				SamplingRate: 1.1,
			},
			wantErr: true,
		},
		{
			name: "invalid trace endpoint",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test",
				SamplingRate:  1.0,
				TraceEndpoint: "invalid-url",
				BatchTimeout:  time.Second,
				BatchSize:     100,
				ExportTimeout: 30 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "invalid batch timeout",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test",
				SamplingRate:  1.0,
				BatchTimeout:  -time.Second,
				BatchSize:     100,
				ExportTimeout: 30 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "invalid batch size",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test",
				SamplingRate:  1.0,
				BatchTimeout:  time.Second,
				BatchSize:     -1,
				ExportTimeout: 30 * time.Second,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		{
			name:     "empty endpoint is valid",
			endpoint: "",
			wantErr:  false,
		},
		{
			name:     "valid http endpoint",
			endpoint: "http://localhost:4318",
			wantErr:  false,
		},
		{
			name:     "valid https endpoint",
			endpoint: "https://api.example.com:443/v1/traces",
			wantErr:  false,
		},
		{
			name:     "invalid scheme",
			endpoint: "ftp://example.com",
			wantErr:  true,
		},
		{
			name:     "no host",
			endpoint: "http://",
			wantErr:  true,
		},
		{
			name:     "malformed URL",
			endpoint: "not-a-url",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEndpoint(tt.endpoint)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateEndpoint() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	// Save original environment
	originalEnv := make(map[string]string)
	envVars := []string{
		"OTEL_ENABLED",
		"OTEL_SERVICE_NAME",
		"OTEL_SERVICE_VERSION",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_SAMPLING_RATE",
		"OTEL_EXPORTER_OTLP_HEADERS",
		"OTEL_EXPORTER_OTLP_INSECURE",
		"OTEL_EXPORTER_OTLP_TIMEOUT",
		"OTEL_RESOURCE_ATTRIBUTES",
	}

	for _, env := range envVars {
		originalEnv[env] = os.Getenv(env)
		os.Unsetenv(env)
	}

	// Restore environment after test
	defer func() {
		for _, env := range envVars {
			if val, exists := originalEnv[env]; exists {
				os.Setenv(env, val)
			} else {
				os.Unsetenv(env)
			}
		}
	}()

	// Test with environment variables set
	os.Setenv("OTEL_ENABLED", "true")
	os.Setenv("OTEL_SERVICE_NAME", "test-service")
	os.Setenv("OTEL_SERVICE_VERSION", "1.0.0")
	os.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318/v1/traces")
	os.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://localhost:4318/v1/metrics")
	os.Setenv("OTEL_SAMPLING_RATE", "0.5")
	os.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "api-key=secret,x-custom=value")
	os.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	os.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "10s")
	os.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=123,deployment.environment=test")

	config := DefaultOTELConfig()
	config.LoadFromEnvironment()

	if !config.Enabled {
		t.Error("Expected Enabled to be true")
	}

	if config.ServiceName != "test-service" {
		t.Errorf("Expected ServiceName to be 'test-service', got %s", config.ServiceName)
	}

	if config.ServiceVersion != "1.0.0" {
		t.Errorf("Expected ServiceVersion to be '1.0.0', got %s", config.ServiceVersion)
	}

	if config.TraceEndpoint != "http://localhost:4318/v1/traces" {
		t.Errorf("Expected TraceEndpoint to be 'http://localhost:4318/v1/traces', got %s", config.TraceEndpoint)
	}

	if config.MetricEndpoint != "http://localhost:4318/v1/metrics" {
		t.Errorf("Expected MetricEndpoint to be 'http://localhost:4318/v1/metrics', got %s", config.MetricEndpoint)
	}

	if config.SamplingRate != 0.5 {
		t.Errorf("Expected SamplingRate to be 0.5, got %f", config.SamplingRate)
	}

	if !config.Insecure {
		t.Error("Expected Insecure to be true")
	}

	if config.BatchTimeout != 10*time.Second {
		t.Errorf("Expected BatchTimeout to be 10s, got %v", config.BatchTimeout)
	}

	expectedHeaders := map[string]string{
		"api-key":  "secret",
		"x-custom": "value",
	}
	for key, expectedValue := range expectedHeaders {
		if actualValue, exists := config.Headers[key]; !exists || actualValue != expectedValue {
			t.Errorf("Expected header %s to be %s, got %s", key, expectedValue, actualValue)
		}
	}

	expectedAttrs := map[string]string{
		"service.instance.id":      "123",
		"deployment.environment":   "test",
	}
	for key, expectedValue := range expectedAttrs {
		if actualValue, exists := config.ResourceAttrs[key]; !exists || actualValue != expectedValue {
			t.Errorf("Expected resource attribute %s to be %s, got %s", key, expectedValue, actualValue)
		}
	}
}

func TestNewConfigFromEnvironment(t *testing.T) {
	// Save original environment
	originalEnabled := os.Getenv("OTEL_ENABLED")
	originalServiceName := os.Getenv("OTEL_SERVICE_NAME")

	// Clean up after test
	defer func() {
		if originalEnabled != "" {
			os.Setenv("OTEL_ENABLED", originalEnabled)
		} else {
			os.Unsetenv("OTEL_ENABLED")
		}
		if originalServiceName != "" {
			os.Setenv("OTEL_SERVICE_NAME", originalServiceName)
		} else {
			os.Unsetenv("OTEL_SERVICE_NAME")
		}
	}()

	os.Setenv("OTEL_ENABLED", "true")
	os.Setenv("OTEL_SERVICE_NAME", "env-service")

	config := NewConfigFromEnvironment()

	if !config.Enabled {
		t.Error("Expected Enabled to be true")
	}

	if config.ServiceName != "env-service" {
		t.Errorf("Expected ServiceName to be 'env-service', got %s", config.ServiceName)
	}
}

func TestParseHeaders(t *testing.T) {
	tests := []struct {
		name      string
		headerStr string
		expected  map[string]string
	}{
		{
			name:      "empty string",
			headerStr: "",
			expected:  map[string]string{},
		},
		{
			name:      "single header",
			headerStr: "api-key=secret",
			expected:  map[string]string{"api-key": "secret"},
		},
		{
			name:      "multiple headers",
			headerStr: "api-key=secret,x-custom=value,authorization=bearer token",
			expected: map[string]string{
				"api-key":       "secret",
				"x-custom":      "value",
				"authorization": "bearer token",
			},
		},
		{
			name:      "headers with spaces",
			headerStr: " api-key = secret , x-custom = value ",
			expected: map[string]string{
				"api-key":  "secret",
				"x-custom": "value",
			},
		},
		{
			name:      "malformed header ignored",
			headerStr: "api-key=secret,malformed,x-custom=value",
			expected: map[string]string{
				"api-key":  "secret",
				"x-custom": "value",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseHeaders(tt.headerStr)
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d headers, got %d", len(tt.expected), len(result))
			}
			for key, expectedValue := range tt.expected {
				if actualValue, exists := result[key]; !exists || actualValue != expectedValue {
					t.Errorf("Expected header %s to be %s, got %s", key, expectedValue, actualValue)
				}
			}
		})
	}
}

func TestParseResourceAttributes(t *testing.T) {
	tests := []struct {
		name     string
		attrStr  string
		expected map[string]string
	}{
		{
			name:     "empty string",
			attrStr:  "",
			expected: map[string]string{},
		},
		{
			name:     "single attribute",
			attrStr:  "service.name=tdcli",
			expected: map[string]string{"service.name": "tdcli"},
		},
		{
			name:     "multiple attributes",
			attrStr:  "service.name=tdcli,service.version=1.0.0,deployment.environment=prod",
			expected: map[string]string{
				"service.name":             "tdcli",
				"service.version":          "1.0.0",
				"deployment.environment":   "prod",
			},
		},
		{
			name:     "attributes with spaces",
			attrStr:  " service.name = tdcli , service.version = 1.0.0 ",
			expected: map[string]string{
				"service.name":    "tdcli",
				"service.version": "1.0.0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseResourceAttributes(tt.attrStr)
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d attributes, got %d", len(tt.expected), len(result))
			}
			for key, expectedValue := range tt.expected {
				if actualValue, exists := result[key]; !exists || actualValue != expectedValue {
					t.Errorf("Expected attribute %s to be %s, got %s", key, expectedValue, actualValue)
				}
			}
		})
	}
}
func TestOTELConfigAdvancedValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *OTELConfig
		wantErr bool
	}{
		{
			name: "valid advanced config",
			config: &OTELConfig{
				Enabled:          true,
				ServiceName:      "test",
				SamplingRate:     1.0,
				BatchTimeout:     time.Second,
				BatchSize:        100,
				ExportTimeout:    30 * time.Second,
				MaxRetryAttempts: 3,
				RetryDelay:       time.Second,
				Compression:      "gzip",
			},
			wantErr: false,
		},
		{
			name: "invalid export timeout",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test",
				SamplingRate:  1.0,
				BatchTimeout:  time.Second,
				BatchSize:     100,
				ExportTimeout: -time.Second,
			},
			wantErr: true,
		},
		{
			name: "invalid max retry attempts",
			config: &OTELConfig{
				Enabled:          true,
				ServiceName:      "test",
				SamplingRate:     1.0,
				BatchTimeout:     time.Second,
				BatchSize:        100,
				ExportTimeout:    30 * time.Second,
				MaxRetryAttempts: -1,
			},
			wantErr: true,
		},
		{
			name: "invalid retry delay",
			config: &OTELConfig{
				Enabled:          true,
				ServiceName:      "test",
				SamplingRate:     1.0,
				BatchTimeout:     time.Second,
				BatchSize:        100,
				ExportTimeout:    30 * time.Second,
				MaxRetryAttempts: 3,
				RetryDelay:       -time.Second,
			},
			wantErr: true,
		},
		{
			name: "invalid compression",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test",
				SamplingRate:  1.0,
				BatchTimeout:  time.Second,
				BatchSize:     100,
				ExportTimeout: 30 * time.Second,
				Compression:   "invalid",
			},
			wantErr: true,
		},
		{
			name: "valid compression none",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test",
				SamplingRate:  1.0,
				BatchTimeout:  time.Second,
				BatchSize:     100,
				ExportTimeout: 30 * time.Second,
				Compression:   "none",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadAdvancedEnvironmentVariables(t *testing.T) {
	// Save original environment
	originalEnv := make(map[string]string)
	envVars := []string{
		"OTEL_EXPORTER_OTLP_EXPORT_TIMEOUT",
		"OTEL_EXPORTER_OTLP_RETRY_ENABLED",
		"OTEL_EXPORTER_OTLP_MAX_RETRY_ATTEMPTS",
		"OTEL_EXPORTER_OTLP_RETRY_DELAY",
		"OTEL_EXPORTER_OTLP_COMPRESSION",
	}

	for _, env := range envVars {
		originalEnv[env] = os.Getenv(env)
		os.Unsetenv(env)
	}

	// Restore environment after test
	defer func() {
		for _, env := range envVars {
			if val, exists := originalEnv[env]; exists {
				os.Setenv(env, val)
			} else {
				os.Unsetenv(env)
			}
		}
	}()

	// Test with advanced environment variables set
	os.Setenv("OTEL_EXPORTER_OTLP_EXPORT_TIMEOUT", "60s")
	os.Setenv("OTEL_EXPORTER_OTLP_RETRY_ENABLED", "false")
	os.Setenv("OTEL_EXPORTER_OTLP_MAX_RETRY_ATTEMPTS", "5")
	os.Setenv("OTEL_EXPORTER_OTLP_RETRY_DELAY", "2s")
	os.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "none")

	config := DefaultOTELConfig()
	config.LoadFromEnvironment()

	if config.ExportTimeout != 60*time.Second {
		t.Errorf("Expected ExportTimeout to be 60s, got %v", config.ExportTimeout)
	}

	if config.RetryEnabled {
		t.Error("Expected RetryEnabled to be false")
	}

	if config.MaxRetryAttempts != 5 {
		t.Errorf("Expected MaxRetryAttempts to be 5, got %d", config.MaxRetryAttempts)
	}

	if config.RetryDelay != 2*time.Second {
		t.Errorf("Expected RetryDelay to be 2s, got %v", config.RetryDelay)
	}

	if config.Compression != "none" {
		t.Errorf("Expected Compression to be 'none', got %s", config.Compression)
	}
}

func TestDefaultOTELConfigAdvancedSettings(t *testing.T) {
	config := DefaultOTELConfig()

	if config.ExportTimeout != 30*time.Second {
		t.Errorf("Expected default ExportTimeout to be 30s, got %v", config.ExportTimeout)
	}

	if !config.RetryEnabled {
		t.Error("Expected default RetryEnabled to be true")
	}

	if config.MaxRetryAttempts != 3 {
		t.Errorf("Expected default MaxRetryAttempts to be 3, got %d", config.MaxRetryAttempts)
	}

	if config.RetryDelay != time.Second {
		t.Errorf("Expected default RetryDelay to be 1s, got %v", config.RetryDelay)
	}

	if config.Compression != "gzip" {
		t.Errorf("Expected default Compression to be 'gzip', got %s", config.Compression)
	}
}
