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
	Enabled           bool
	ServiceName       string
	ServiceVersion    string
	TraceEndpoint     string
	MetricEndpoint    string
	SamplingRate      float64
	Headers           map[string]string
	Insecure          bool
	BatchTimeout      time.Duration
	BatchSize         int
	ResourceAttrs     map[string]string
	
	// Advanced exporter settings
	ExportTimeout     time.Duration
	RetryEnabled      bool
	MaxRetryAttempts  int
	RetryDelay        time.Duration
	Compression       string
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
	}

	if c.MetricEndpoint != "" {
		if err := validateEndpoint(c.MetricEndpoint); err != nil {
			return fmt.Errorf("invalid metric endpoint: %w", err)
		}
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
