package treasuredata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func TestClientWithOTEL(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"databases": []}`))
	}))
	defer server.Close()

	// Set up tracing
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("test"),
		)),
	)
	tracer := tp.Tracer("test")

	// Set up metrics
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	meter := mp.Meter("test")

	// Create client with OTEL instrumentation
	client, err := NewClient("test-api-key",
		WithEndpoint(server.URL),
		WithOTEL(tracer, meter),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Verify OTEL is enabled
	if !client.IsOTELEnabled() {
		t.Error("Expected OTEL to be enabled")
	}

	if client.GetTracer() != tracer {
		t.Error("Expected tracer to match")
	}

	if client.GetMeter() != meter {
		t.Error("Expected meter to match")
	}

	// Make a request to verify instrumentation works
	req, err := client.NewRequest("GET", "/v3/databases", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := client.Do(context.Background(), req, nil)
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

	// Verify TD-specific attributes
	attrs := span.Attributes
	foundAPIVersion := false
	foundEndpoint := false

	for _, attr := range attrs {
		switch string(attr.Key) {
		case "td.api_version":
			foundAPIVersion = true
			if attr.Value.AsString() != "v3" {
				t.Errorf("Expected td.api_version=v3, got %s", attr.Value.AsString())
			}
		case "td.endpoint":
			foundEndpoint = true
			if attr.Value.AsString() != "databases" {
				t.Errorf("Expected td.endpoint=databases, got %s", attr.Value.AsString())
			}
		}
	}

	if !foundAPIVersion {
		t.Error("Expected td.api_version attribute not found")
	}
	if !foundEndpoint {
		t.Error("Expected td.endpoint attribute not found")
	}
}

func TestClientWithoutOTEL(t *testing.T) {
	// Create client without OTEL instrumentation
	client, err := NewClient("test-api-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Verify OTEL is not enabled
	if client.IsOTELEnabled() {
		t.Error("Expected OTEL to be disabled")
	}

	if client.GetTracer() != nil {
		t.Error("Expected tracer to be nil")
	}

	if client.GetMeter() != nil {
		t.Error("Expected meter to be nil")
	}
}

func TestClientOTELPartialConfiguration(t *testing.T) {
	// Test with only tracer (should not enable OTEL)
	tracer := otel.Tracer("test")

	client, err := NewClient("test-api-key", WithOTEL(tracer, nil))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	if client.IsOTELEnabled() {
		t.Error("Expected OTEL to be disabled with partial configuration")
	}

	// Test with only meter (should not enable OTEL)
	meter := otel.Meter("test")

	client2, err := NewClient("test-api-key", WithOTEL(nil, meter))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	if client2.IsOTELEnabled() {
		t.Error("Expected OTEL to be disabled with partial configuration")
	}
}
