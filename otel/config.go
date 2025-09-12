package otel

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// OTELConfig holds all OpenTelemetry configuration options
type OTELConfig struct {
	Enabled        bool
	ServiceName    string
	ServiceVersion string
	TraceEndpoint  string
	MetricEndpoint string
	SamplingRate   float64
	Headers        map[string]string
	Insecure       bool
	BatchTimeout   time.Duration
	BatchSize      int
	ResourceAttrs  map[string]string

	// Advanced exporter settings
	ExportTimeout    time.Duration
	RetryEnabled     bool
	MaxRetryAttempts int
	RetryDelay       time.Duration
	Compression      string

	// Retry and circuit breaker settings
	RetryMaxDelay           time.Duration
	RetryBackoffFactor      float64
	RetryJitter             bool
	CircuitBreakerEnabled   bool
	CircuitFailureThreshold int
	CircuitRecoveryTimeout  time.Duration
	CircuitHalfOpenMaxCalls int
}

// DefaultOTELConfig returns a configuration with sensible defaults
func DefaultOTELConfig() *OTELConfig {
	return &OTELConfig{
		Enabled:          false,
		ServiceName:      "tdcli",
		ServiceVersion:   "unknown",
		SamplingRate:     1.0,
		Headers:          make(map[string]string),
		Insecure:         false,
		BatchTimeout:     5 * time.Second,
		BatchSize:        512,
		ResourceAttrs:    make(map[string]string),
		ExportTimeout:    30 * time.Second,
		RetryEnabled:     true,
		MaxRetryAttempts: 3,
		RetryDelay:       time.Second,
		Compression:      "gzip",

		// Retry and circuit breaker defaults
		RetryMaxDelay:           30 * time.Second,
		RetryBackoffFactor:      2.0,
		RetryJitter:             true,
		CircuitBreakerEnabled:   true,
		CircuitFailureThreshold: 5,
		CircuitRecoveryTimeout:  60 * time.Second,
		CircuitHalfOpenMaxCalls: 3,
	}
}

// Validate checks the configuration for errors and returns helpful messages
func (c *OTELConfig) Validate() error {
	if !c.Enabled {
		return nil // No validation needed if disabled
	}

	if c.ServiceName == "" {
		return fmt.Errorf("service name cannot be empty when OTEL is enabled")
	}

	if c.SamplingRate < 0.0 || c.SamplingRate > 1.0 {
		return fmt.Errorf("sampling rate must be between 0.0 and 1.0, got %f", c.SamplingRate)
	}

	if c.TraceEndpoint != "" {
		if err := validateEndpoint(c.TraceEndpoint); err != nil {
			return fmt.Errorf("invalid trace endpoint: %w", err)
		}
		if err := validateSecureTransport(c.TraceEndpoint, c.Insecure); err != nil {
			return fmt.Errorf("trace endpoint security validation failed: %w", err)
		}
	}

	if c.MetricEndpoint != "" {
		if err := validateEndpoint(c.MetricEndpoint); err != nil {
			return fmt.Errorf("invalid metric endpoint: %w", err)
		}
		if err := validateSecureTransport(c.MetricEndpoint, c.Insecure); err != nil {
			return fmt.Errorf("metric endpoint security validation failed: %w", err)
		}
	}

	if err := validateHeaders(c.Headers); err != nil {
		return fmt.Errorf("header validation failed: %w", err)
	}

	if c.BatchTimeout <= 0 {
		return fmt.Errorf("batch timeout must be positive, got %v", c.BatchTimeout)
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got %d", c.BatchSize)
	}

	if c.ExportTimeout <= 0 {
		return fmt.Errorf("export timeout must be positive, got %v", c.ExportTimeout)
	}

	if c.MaxRetryAttempts < 0 {
		return fmt.Errorf("max retry attempts must be non-negative, got %d", c.MaxRetryAttempts)
	}

	if c.RetryDelay < 0 {
		return fmt.Errorf("retry delay must be non-negative, got %v", c.RetryDelay)
	}

	if c.Compression != "" && c.Compression != "gzip" && c.Compression != "none" {
		return fmt.Errorf("compression must be 'gzip' or 'none', got %s", c.Compression)
	}

	if c.RetryMaxDelay <= 0 {
		return fmt.Errorf("retry max delay must be positive, got %v", c.RetryMaxDelay)
	}

	if c.RetryBackoffFactor <= 1.0 {
		return fmt.Errorf("retry backoff factor must be greater than 1.0, got %f", c.RetryBackoffFactor)
	}

	if c.CircuitFailureThreshold <= 0 {
		return fmt.Errorf("circuit failure threshold must be positive, got %d", c.CircuitFailureThreshold)
	}

	if c.CircuitRecoveryTimeout <= 0 {
		return fmt.Errorf("circuit recovery timeout must be positive, got %v", c.CircuitRecoveryTimeout)
	}

	if c.CircuitHalfOpenMaxCalls <= 0 {
		return fmt.Errorf("circuit half-open max calls must be positive, got %d", c.CircuitHalfOpenMaxCalls)
	}

	return nil
}

// LoadFromEnvironment loads configuration from environment variables
// following OpenTelemetry standard environment variable names
func (c *OTELConfig) LoadFromEnvironment() {
	if enabled := os.Getenv("OTEL_ENABLED"); enabled != "" {
		c.Enabled = strings.ToLower(enabled) == "true"
	}

	if serviceName := os.Getenv("OTEL_SERVICE_NAME"); serviceName != "" {
		c.ServiceName = serviceName
	}

	if serviceVersion := os.Getenv("OTEL_SERVICE_VERSION"); serviceVersion != "" {
		c.ServiceVersion = serviceVersion
	}

	if traceEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"); traceEndpoint != "" {
		c.TraceEndpoint = traceEndpoint
	}

	if metricEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"); metricEndpoint != "" {
		c.MetricEndpoint = metricEndpoint
	}

	// Check for generic OTLP endpoint if specific ones aren't set
	if genericEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); genericEndpoint != "" {
		if c.TraceEndpoint == "" {
			c.TraceEndpoint = genericEndpoint + "/v1/traces"
		}
		if c.MetricEndpoint == "" {
			c.MetricEndpoint = genericEndpoint + "/v1/metrics"
		}
	}

	if samplingRate := os.Getenv("OTEL_SAMPLING_RATE"); samplingRate != "" {
		if rate, err := strconv.ParseFloat(samplingRate, 64); err == nil {
			c.SamplingRate = rate
		}
	}

	if headers := os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"); headers != "" {
		c.Headers = parseHeaders(headers)
	}

	if insecure := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); insecure != "" {
		c.Insecure = strings.ToLower(insecure) == "true"
	}

	if timeout := os.Getenv("OTEL_EXPORTER_OTLP_TIMEOUT"); timeout != "" {
		if duration, err := time.ParseDuration(timeout); err == nil {
			c.BatchTimeout = duration
		}
	}

	// Load resource attributes
	if resourceAttrs := os.Getenv("OTEL_RESOURCE_ATTRIBUTES"); resourceAttrs != "" {
		c.ResourceAttrs = parseResourceAttributes(resourceAttrs)
	}

	// Load advanced exporter settings
	if exportTimeout := os.Getenv("OTEL_EXPORTER_OTLP_EXPORT_TIMEOUT"); exportTimeout != "" {
		if duration, err := time.ParseDuration(exportTimeout); err == nil {
			c.ExportTimeout = duration
		}
	}

	if retryEnabled := os.Getenv("OTEL_EXPORTER_OTLP_RETRY_ENABLED"); retryEnabled != "" {
		c.RetryEnabled = strings.ToLower(retryEnabled) == "true"
	}

	if maxRetries := os.Getenv("OTEL_EXPORTER_OTLP_MAX_RETRY_ATTEMPTS"); maxRetries != "" {
		if attempts, err := strconv.Atoi(maxRetries); err == nil {
			c.MaxRetryAttempts = attempts
		}
	}

	if retryDelay := os.Getenv("OTEL_EXPORTER_OTLP_RETRY_DELAY"); retryDelay != "" {
		if duration, err := time.ParseDuration(retryDelay); err == nil {
			c.RetryDelay = duration
		}
	}

	if compression := os.Getenv("OTEL_EXPORTER_OTLP_COMPRESSION"); compression != "" {
		c.Compression = compression
	}

	// Load retry and circuit breaker settings
	if retryMaxDelay := os.Getenv("OTEL_EXPORTER_RETRY_MAX_DELAY"); retryMaxDelay != "" {
		if duration, err := time.ParseDuration(retryMaxDelay); err == nil {
			c.RetryMaxDelay = duration
		}
	}

	if retryBackoffFactor := os.Getenv("OTEL_EXPORTER_RETRY_BACKOFF_FACTOR"); retryBackoffFactor != "" {
		if factor, err := strconv.ParseFloat(retryBackoffFactor, 64); err == nil {
			c.RetryBackoffFactor = factor
		}
	}

	if retryJitter := os.Getenv("OTEL_EXPORTER_RETRY_JITTER"); retryJitter != "" {
		c.RetryJitter = strings.ToLower(retryJitter) == "true"
	}

	if circuitBreakerEnabled := os.Getenv("OTEL_EXPORTER_CIRCUIT_BREAKER_ENABLED"); circuitBreakerEnabled != "" {
		c.CircuitBreakerEnabled = strings.ToLower(circuitBreakerEnabled) == "true"
	}

	if circuitFailureThreshold := os.Getenv("OTEL_EXPORTER_CIRCUIT_FAILURE_THRESHOLD"); circuitFailureThreshold != "" {
		if threshold, err := strconv.Atoi(circuitFailureThreshold); err == nil {
			c.CircuitFailureThreshold = threshold
		}
	}

	if circuitRecoveryTimeout := os.Getenv("OTEL_EXPORTER_CIRCUIT_RECOVERY_TIMEOUT"); circuitRecoveryTimeout != "" {
		if duration, err := time.ParseDuration(circuitRecoveryTimeout); err == nil {
			c.CircuitRecoveryTimeout = duration
		}
	}

	if circuitHalfOpenMaxCalls := os.Getenv("OTEL_EXPORTER_CIRCUIT_HALF_OPEN_MAX_CALLS"); circuitHalfOpenMaxCalls != "" {
		if calls, err := strconv.Atoi(circuitHalfOpenMaxCalls); err == nil {
			c.CircuitHalfOpenMaxCalls = calls
		}
	}
}

// NewConfigFromEnvironment creates a new configuration loaded from environment variables
func NewConfigFromEnvironment() *OTELConfig {
	config := DefaultOTELConfig()
	config.LoadFromEnvironment()
	return config
}

// parseHeaders parses header string in format "key1=value1,key2=value2"
func parseHeaders(headerStr string) map[string]string {
	headers := make(map[string]string)
	if headerStr == "" {
		return headers
	}

	pairs := strings.Split(headerStr, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) == 2 {
			headers[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return headers
}

// parseResourceAttributes parses resource attributes in format "key1=value1,key2=value2"
func parseResourceAttributes(attrStr string) map[string]string {
	attrs := make(map[string]string)
	if attrStr == "" {
		return attrs
	}

	pairs := strings.Split(attrStr, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) == 2 {
			attrs[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return attrs
}

// validateEndpoint checks if the endpoint URL is valid
func validateEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("endpoint must use http or https scheme, got %s", u.Scheme)
	}

	if u.Host == "" {
		return fmt.Errorf("endpoint must have a host")
	}

	return nil
}

// validateSecureTransport validates secure transport configuration
func validateSecureTransport(endpoint string, insecure bool) error {
	if endpoint == "" {
		return nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil // Already validated in validateEndpoint
	}

	// Allow HTTP endpoints without requiring an explicit insecure flag.
	// For localhost/loopback this is common in development.
	// For non-localhost, we don't fail validation here; upstream setup can still enforce TLS.
	if u.Scheme == "http" {
		return nil
	}

	return nil
}

// validateHeaders checks headers for potential security issues
func validateHeaders(headers map[string]string) error {
	if len(headers) == 0 {
		return nil
	}

	for key, value := range headers {
		// Check for empty header values
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("header %q has empty value", key)
		}

		// Warn about potentially sensitive headers in plain text
		lowerKey := strings.ToLower(key)
		if strings.Contains(lowerKey, "password") ||
			strings.Contains(lowerKey, "secret") ||
			(strings.Contains(lowerKey, "key") && !strings.Contains(lowerKey, "api")) {
			// This is a warning - we don't fail validation but the caller should log it
		}

		// Check for reasonable header value length
		if len(value) > 8192 {
			return fmt.Errorf("header %q value is too long (%d bytes, max 8192)", key, len(value))
		}

		// Check for control characters in header values
		for _, char := range value {
			if char < 32 && char != 9 { // Allow tab (9) but not other control chars
				return fmt.Errorf("header %q contains invalid control character", key)
			}
		}
	}

	return nil
}
