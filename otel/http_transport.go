package otel

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// otelHTTPTransport wraps an http.RoundTripper with OpenTelemetry instrumentation
type otelHTTPTransport struct {
	base   http.RoundTripper
	tracer trace.Tracer
	meter  metric.Meter

	// Metrics
	requestDuration metric.Float64Histogram
	requestCounter  metric.Int64Counter
	requestSize     metric.Int64Histogram
	responseSize    metric.Int64Histogram
	errorCounter    metric.Int64Counter
}

// NewOTELHTTPTransport creates a new instrumented HTTP transport
func NewOTELHTTPTransport(base http.RoundTripper, tracer trace.Tracer, meter metric.Meter) (http.RoundTripper, error) {
	if base == nil {
		base = http.DefaultTransport
	}

	transport := &otelHTTPTransport{
		base:   base,
		tracer: tracer,
		meter:  meter,
	}

	// Initialize metrics
	if err := transport.initializeMetrics(); err != nil {
		return nil, fmt.Errorf("failed to initialize HTTP transport metrics: %w", err)
	}

	return transport, nil
}

// initializeMetrics creates the metric instruments for HTTP requests
func (t *otelHTTPTransport) initializeMetrics() error {
	var err error

	// Request duration histogram
	t.requestDuration, err = t.meter.Float64Histogram(
		"http_request_duration",
		metric.WithDescription("Duration of HTTP requests in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("failed to create request duration histogram: %w", err)
	}

	// Request counter
	t.requestCounter, err = t.meter.Int64Counter(
		"http_requests_total",
		metric.WithDescription("Total number of HTTP requests"),
	)
	if err != nil {
		return fmt.Errorf("failed to create request counter: %w", err)
	}

	// Request size histogram
	t.requestSize, err = t.meter.Int64Histogram(
		"http_request_size_bytes",
		metric.WithDescription("Size of HTTP request bodies in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("failed to create request size histogram: %w", err)
	}

	// Response size histogram
	t.responseSize, err = t.meter.Int64Histogram(
		"http_response_size_bytes",
		metric.WithDescription("Size of HTTP response bodies in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("failed to create response size histogram: %w", err)
	}

	// Error counter
	t.errorCounter, err = t.meter.Int64Counter(
		"http_errors_total",
		metric.WithDescription("Total number of HTTP request errors"),
	)
	if err != nil {
		return fmt.Errorf("failed to create error counter: %w", err)
	}

	return nil
}

// RoundTrip implements the http.RoundTripper interface with instrumentation
func (t *otelHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	startTime := time.Now()

	// Create span for the HTTP request
	ctx, span := t.tracer.Start(req.Context(), "http.request",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	// Add standard HTTP span attributes
	t.addHTTPSpanAttributes(span, req)

	// Execute request with instrumented context
	req = req.WithContext(ctx)
	resp, err := t.base.RoundTrip(req)

	// Calculate duration
	duration := time.Since(startTime)

	// Record metrics and update span
	t.recordMetrics(req, resp, err, duration)
	t.updateSpanWithResponse(span, resp, err)

	return resp, err
}

// addHTTPSpanAttributes adds standard HTTP attributes to the span
func (t *otelHTTPTransport) addHTTPSpanAttributes(span trace.Span, req *http.Request) {
	// Standard HTTP semantic conventions
	span.SetAttributes(
		attribute.String("http.request.method", req.Method),
		attribute.String("url.full", t.sanitizeURL(req.URL)),
		attribute.String("url.scheme", req.URL.Scheme),
		attribute.String("server.address", req.URL.Host),
		attribute.String("url.path", req.URL.Path),
		attribute.String("user_agent.original", req.UserAgent()),
	)

	// Add query parameters (sanitized)
	if req.URL.RawQuery != "" {
		span.SetAttributes(attribute.String("url.query", t.sanitizeQuery(req.URL.RawQuery)))
	}

	// Add request content length if available
	if req.ContentLength > 0 {
		span.SetAttributes(semconv.HTTPRequestSize(int(req.ContentLength)))
	}

	// Add custom TD-specific attributes
	t.addTDSpecificAttributes(span, req)
}

// addTDSpecificAttributes adds Treasure Data specific attributes to spans
func (t *otelHTTPTransport) addTDSpecificAttributes(span trace.Span, req *http.Request) {
	// Extract API version from URL path
	if apiVersion := t.extractAPIVersion(req.URL.Path); apiVersion != "" {
		span.SetAttributes(attribute.String("td.api_version", apiVersion))
	}

	// Extract endpoint from URL path
	if endpoint := t.extractEndpoint(req.URL.Path); endpoint != "" {
		span.SetAttributes(attribute.String("td.endpoint", endpoint))
	}

	// Add region information if available in host
	if region := t.extractRegion(req.URL.Host); region != "" {
		span.SetAttributes(attribute.String("td.region", region))
	}

	// Add service type based on host
	if serviceType := t.extractServiceType(req.URL.Host); serviceType != "" {
		span.SetAttributes(attribute.String("td.service_type", serviceType))
	}
}

// updateSpanWithResponse updates the span with response information
func (t *otelHTTPTransport) updateSpanWithResponse(span trace.Span, resp *http.Response, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	if resp != nil {
		// Add response attributes
		span.SetAttributes(
			semconv.HTTPResponseStatusCode(resp.StatusCode),
		)

		// Add response content length if available
		if resp.ContentLength > 0 {
			span.SetAttributes(semconv.HTTPResponseSize(int(resp.ContentLength)))
		}

		// Set span status based on HTTP status code
		if resp.StatusCode >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", resp.StatusCode))
		} else {
			span.SetStatus(codes.Ok, "")
		}
	}
}

// recordMetrics records HTTP request metrics
func (t *otelHTTPTransport) recordMetrics(req *http.Request, resp *http.Response, err error, duration time.Duration) {
	// Common metric attributes
	attrs := []attribute.KeyValue{
		attribute.String("http.method", req.Method),
		attribute.String("http.scheme", req.URL.Scheme),
		attribute.String("http.host", req.URL.Host),
	}

	// Add TD-specific attributes
	if apiVersion := t.extractAPIVersion(req.URL.Path); apiVersion != "" {
		attrs = append(attrs, attribute.String("td.api_version", apiVersion))
	}
	if endpoint := t.extractEndpoint(req.URL.Path); endpoint != "" {
		attrs = append(attrs, attribute.String("td.endpoint", endpoint))
	}

	// Record request duration
	t.requestDuration.Record(req.Context(), duration.Seconds(), metric.WithAttributes(attrs...))

	// Add status code to attributes if response is available
	if resp != nil {
		attrs = append(attrs, attribute.Int("http.status_code", resp.StatusCode))

		// Record response size
		if resp.ContentLength > 0 {
			t.responseSize.Record(req.Context(), resp.ContentLength, metric.WithAttributes(attrs...))
		}
	}

	// Record request counter
	t.requestCounter.Add(req.Context(), 1, metric.WithAttributes(attrs...))

	// Record request size
	if req.ContentLength > 0 {
		t.requestSize.Record(req.Context(), req.ContentLength, metric.WithAttributes(attrs...))
	}

	// Record errors
	if err != nil || (resp != nil && resp.StatusCode >= 400) {
		errorAttrs := append(attrs, attribute.String("error.type", t.getErrorType(err, resp)))
		t.errorCounter.Add(req.Context(), 1, metric.WithAttributes(errorAttrs...))
	}
}

// sanitizeURL removes sensitive information from URLs for span attributes
func (t *otelHTTPTransport) sanitizeURL(u *url.URL) string {
	if u == nil {
		return ""
	}

	// Create a copy to avoid modifying the original
	sanitized := *u

	// Remove query parameters that might contain sensitive data
	if sanitized.RawQuery != "" {
		sanitized.RawQuery = t.sanitizeQuery(sanitized.RawQuery)
	}

	// Remove fragment
	sanitized.Fragment = ""

	return sanitized.String()
}

// sanitizeQuery removes sensitive query parameters
func (t *otelHTTPTransport) sanitizeQuery(query string) string {
	values, err := url.ParseQuery(query)
	if err != nil {
		return "[invalid_query]"
	}

	// List of sensitive parameter names to mask
	sensitiveParams := map[string]bool{
		"api_key":    true,
		"apikey":     true,
		"token":      true,
		"password":   true,
		"secret":     true,
		"key":        true,
		"auth":       true,
		"credential": true,
	}

	// Sanitize sensitive parameters
	for key := range values {
		if sensitiveParams[strings.ToLower(key)] {
			values.Set(key, "[REDACTED]")
		}
	}

	return values.Encode()
}

// extractAPIVersion extracts API version from URL path
func (t *otelHTTPTransport) extractAPIVersion(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for _, part := range parts {
		if strings.HasPrefix(part, "v") && len(part) > 1 {
			if _, err := strconv.Atoi(part[1:]); err == nil {
				return part
			}
		}
	}
	return ""
}

// extractEndpoint extracts the API endpoint from URL path
func (t *otelHTTPTransport) extractEndpoint(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}

	// Find the first part that looks like an endpoint (not "api" or version)
	for _, part := range parts {
		// Skip "api" prefix
		if part == "api" {
			continue
		}
		// Skip version parts (v1, v2, etc.)
		if strings.HasPrefix(part, "v") && len(part) > 1 {
			if _, err := strconv.Atoi(part[1:]); err == nil {
				continue
			}
		}
		// This should be the endpoint
		return part
	}

	return ""
}

// extractRegion extracts region information from hostname
func (t *otelHTTPTransport) extractRegion(host string) string {
	// Extract region from TD hostnames
	if strings.Contains(host, "eu01") {
		return "eu"
	}
	if strings.Contains(host, "ap02") {
		return "ap02"
	}
	if strings.Contains(host, "treasuredata.co.jp") {
		return "tokyo"
	}
	if strings.Contains(host, "treasuredata.com") {
		return "us"
	}
	return ""
}

// extractServiceType extracts service type from hostname
func (t *otelHTTPTransport) extractServiceType(host string) string {
	if strings.Contains(host, "api-cdp") {
		return "cdp"
	}
	if strings.Contains(host, "api-workflow") {
		return "workflow"
	}
	if strings.Contains(host, "api") {
		return "core"
	}
	return ""
}

// getErrorType categorizes errors for metrics
func (t *otelHTTPTransport) getErrorType(err error, resp *http.Response) string {
	if err != nil {
		if strings.Contains(err.Error(), "timeout") {
			return "timeout"
		}
		if strings.Contains(err.Error(), "connection") {
			return "connection"
		}
		if strings.Contains(err.Error(), "dns") {
			return "dns"
		}
		return "network"
	}

	if resp != nil {
		switch {
		case resp.StatusCode >= 500:
			return "server_error"
		case resp.StatusCode >= 400:
			return "client_error"
		}
	}

	return "unknown"
}
