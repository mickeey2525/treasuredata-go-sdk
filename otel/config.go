package otel

import (
	"fmt"
	"net/url"
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
}

// DefaultOTELConfig returns a configuration with sensible defaults
func DefaultOTELConfig() *OTELConfig {
	return &OTELConfig{
		Enabled:        false,
		ServiceName:    "tdcli",
		ServiceVersion: "unknown",
		SamplingRate:   1.0,
		Headers:        make(map[string]string),
		Insecure:       false,
		BatchTimeout:   5 * time.Second,
		BatchSize:      512,
		ResourceAttrs:  make(map[string]string),
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

	return nil
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
