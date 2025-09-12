package otel

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func TestNewOTELHTTPTransport(t *testing.T) {
	tracer := otel.Tracer("test")
	meter := otel.Meter("test")

	transport, err := NewOTELHTTPTransport(nil, tracer, meter)
	if err != nil {
		t.Fatalf("Failed to create OTEL HTTP transport: %v", err)
	}

	if transport == nil {
		t.Fatal("Expected non-nil transport")
	}

	// Test with custom base transport
	customBase := &http.Transport{}
	transport2, err := NewOTELHTTPTransport(customBase, tracer, meter)
	if err != nil {
		t.Fatalf("Failed to create OTEL HTTP transport with custom base: %v", err)
	}

	otelTransport := transport2.(*otelHTTPTransport)
	if otelTransport.base != customBase {
		t.Error("Expected custom base transport to be used")
	}
}

func TestHTTPTransportRoundTrip(t *testing.T) {
	// In-memory handler; no real server is started
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	})

	// Set up tracing
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("test"),
		)),
	)
	tracer := tp.Tracer("test")

	// Set up metrics
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test")

	// Base transport that routes to the in-memory handler
    base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        resp := rr.Result()
        resp.Request = req
        return resp, nil
    })

	// Create instrumented transport wrapping the in-memory base
	transport, err := NewOTELHTTPTransport(base, tracer, meter)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// Create HTTP client with instrumented transport
	client := &http.Client{Transport: transport}

	// Make request to dummy URL; handled by in-memory transport
	resp, err := client.Get("http://example/v3/databases")
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Verify response
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Verify spans were created
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("Expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Name != "http.request" {
		t.Errorf("Expected span name 'http.request', got '%s'", span.Name)
	}

	// Verify span attributes
	attrs := span.Attributes
	expectedAttrs := map[string]interface{}{
		"http.request.method": "GET",
		"url.scheme":          "http",
		"td.api_version":      "v3",
		"td.endpoint":         "databases",
	}

	for key, expectedValue := range expectedAttrs {
		found := false
		for _, attr := range attrs {
			if string(attr.Key) == key {
				found = true
				if attr.Value.AsString() != expectedValue {
					t.Errorf("Expected %s=%v, got %v", key, expectedValue, attr.Value.AsString())
				}
				break
			}
		}
		if !found {
			t.Errorf("Expected attribute %s not found", key)
		}
	}

	// Verify metrics were recorded
	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("Expected metrics to be recorded")
	}

	// Check that we have the expected metrics
	expectedMetrics := []string{
		"http_request_duration",
		"http_requests_total",
	}

	recordedMetrics := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			recordedMetrics[m.Name] = true
		}
	}

	for _, expected := range expectedMetrics {
		if !recordedMetrics[expected] {
			t.Errorf("Expected metric %s not found", expected)
		}
	}
}

func TestHTTPTransportError(t *testing.T) {
	// Set up tracing
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	tracer := tp.Tracer("test")

	// Set up metrics
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test")

	// Create a base transport that always returns an error (no network)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("connection refused")
	})

	// Create instrumented transport wrapping the failing base
	transport, err := NewOTELHTTPTransport(base, tracer, meter)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// Create HTTP client with instrumented transport
	client := &http.Client{Transport: transport}

	// Make request to dummy URL; failing base simulates network error
	_, err = client.Get("http://example/any")
	if err == nil {
		t.Fatal("Expected request to fail")
	}

	// Verify error span was created
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("Expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Status.Code != codes.Error {
		t.Error("Expected span to have error status")
	}

	// Verify error metrics were recorded
	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	// Check for error counter
	errorCounterFound := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "http_errors_total" {
				errorCounterFound = true
				break
			}
		}
	}

	if !errorCounterFound {
		t.Error("Expected error counter metric to be recorded")
	}
}

// roundTripperFunc helps build a RoundTripper from a function for tests.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSanitizeURL(t *testing.T) {
	transport := &otelHTTPTransport{}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with sensitive query params",
			input:    "https://api.treasuredata.com/v3/databases?api_key=secret123&format=json",
			expected: "https://api.treasuredata.com/v3/databases?api_key=%5BREDACTED%5D&format=json",
		},
		{
			name:     "URL without sensitive params",
			input:    "https://api.treasuredata.com/v3/databases?format=json&limit=10",
			expected: "https://api.treasuredata.com/v3/databases?format=json&limit=10",
		},
		{
			name:     "URL with fragment",
			input:    "https://api.treasuredata.com/v3/databases#section",
			expected: "https://api.treasuredata.com/v3/databases",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.input)
			if err != nil {
				t.Fatalf("Failed to parse URL: %v", err)
			}

			result := transport.sanitizeURL(u)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestExtractAPIVersion(t *testing.T) {
	transport := &otelHTTPTransport{}

	tests := []struct {
		path     string
		expected string
	}{
		{"/v3/databases", "v3"},
		{"/v1/jobs", "v1"},
		{"/databases", ""},
		{"/api/v2/tables", "v2"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := transport.extractAPIVersion(tt.path)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestExtractEndpoint(t *testing.T) {
	transport := &otelHTTPTransport{}

	tests := []struct {
		path     string
		expected string
	}{
		{"/v3/databases", "databases"},
		{"/v1/jobs", "jobs"},
		{"/databases", "databases"},
		{"/api/v2/tables", "tables"},
		{"", ""},
		{"/", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := transport.extractEndpoint(tt.path)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestExtractRegion(t *testing.T) {
	transport := &otelHTTPTransport{}

	tests := []struct {
		host     string
		expected string
	}{
		{"api.treasuredata.com", "us"},
		{"api.eu01.treasuredata.com", "eu"},
		{"api.ap02.treasuredata.com", "ap02"},
		{"api.treasuredata.co.jp", "tokyo"},
		{"api-cdp.us01.treasuredata.com", "us"},
		{"unknown.example.com", ""},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			result := transport.extractRegion(tt.host)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestExtractServiceType(t *testing.T) {
	transport := &otelHTTPTransport{}

	tests := []struct {
		host     string
		expected string
	}{
		{"api.treasuredata.com", "core"},
		{"api-cdp.us01.treasuredata.com", "cdp"},
		{"api-workflow.treasuredata.com", "workflow"},
		{"unknown.example.com", ""},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			result := transport.extractServiceType(tt.host)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestGetErrorType(t *testing.T) {
	transport := &otelHTTPTransport{}

	tests := []struct {
		name     string
		err      error
		resp     *http.Response
		expected string
	}{
		{
			name:     "timeout error",
			err:      fmt.Errorf("request timeout"),
			expected: "timeout",
		},
		{
			name:     "connection error",
			err:      fmt.Errorf("connection refused"),
			expected: "connection",
		},
		{
			name:     "dns error",
			err:      fmt.Errorf("dns lookup failed"),
			expected: "dns",
		},
		{
			name:     "other network error",
			err:      fmt.Errorf("network unreachable"),
			expected: "network",
		},
		{
			name:     "server error",
			resp:     &http.Response{StatusCode: 500},
			expected: "server_error",
		},
		{
			name:     "client error",
			resp:     &http.Response{StatusCode: 400},
			expected: "client_error",
		},
		{
			name:     "unknown error",
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := transport.getErrorType(tt.err, tt.resp)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}
