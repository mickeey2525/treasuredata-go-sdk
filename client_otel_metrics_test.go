package treasuredata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func TestClientOTELMetrics(t *testing.T) {
	// Create test server that returns different responses
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")

		// Return different status codes for different requests
		switch requestCount {
		case 1:
			// Success response
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"databases": [{"name": "test_db"}]}`))
		case 2:
			// Client error
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error": "Bad request"}`))
		case 3:
			// Server error
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "Internal server error"}`))
		}
	}))
	defer server.Close()

	// Set up tracing and metrics
	tp := trace.NewTracerProvider(
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("test"),
		)),
	)
	tracer := tp.Tracer("test")

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

	// Make successful request
	req1, err := client.NewRequest("GET", "/v3/databases", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	resp1, err := client.Do(context.Background(), req1, nil)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	resp1.Body.Close()

	// Make client error request
	req2, err := client.NewRequest("POST", "/v3/jobs", map[string]string{"invalid": "data"})
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	resp2, err := client.Do(context.Background(), req2, nil)
	if err == nil {
		resp2.Body.Close()
	}

	// Make server error request
	req3, err := client.NewRequest("GET", "/v3/tables", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	resp3, err := client.Do(context.Background(), req3, nil)
	if err == nil {
		resp3.Body.Close()
	}

	// Collect metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("Expected metrics to be recorded")
	}

	// Verify expected metrics exist
	expectedMetrics := map[string]bool{
		"http_request_duration":    false,
		"http_requests_total":      false,
		"http_request_size_bytes":  false,
		"http_response_size_bytes": false,
		"http_errors_total":        false,
	}

	// Check recorded metrics
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if _, exists := expectedMetrics[m.Name]; exists {
				expectedMetrics[m.Name] = true

				// Verify metric has proper attributes
				switch m.Name {
				case "http_requests_total":
					verifyRequestCounterMetric(t, m)
				case "http_errors_total":
					verifyErrorCounterMetric(t, m)
				case "http_request_duration":
					verifyDurationHistogramMetric(t, m)
				}
			}
		}
	}

	// Verify all expected metrics were found
	for metricName, found := range expectedMetrics {
		if !found {
			t.Errorf("Expected metric %s not found", metricName)
		}
	}
}

func verifyRequestCounterMetric(t *testing.T, m metricdata.Metrics) {
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Errorf("Expected Sum data for %s", m.Name)
		return
	}

	// Should have at least 3 data points (one for each request)
	if len(sum.DataPoints) < 3 {
		t.Errorf("Expected at least 3 data points for %s, got %d", m.Name, len(sum.DataPoints))
		return
	}

	// Verify attributes are present
	for _, dp := range sum.DataPoints {
		hasMethod := false
		hasEndpoint := false
		hasAPIVersion := false
		hasStatusCode := false

		for _, attr := range dp.Attributes.ToSlice() {
			switch string(attr.Key) {
			case "http.method":
				hasMethod = true
			case "td.endpoint":
				hasEndpoint = true
			case "td.api_version":
				hasAPIVersion = true
			case "http.status_code":
				hasStatusCode = true
			}
		}

		if !hasMethod {
			t.Error("Expected http.method attribute in request counter")
		}
		if !hasEndpoint {
			t.Error("Expected td.endpoint attribute in request counter")
		}
		if !hasAPIVersion {
			t.Error("Expected td.api_version attribute in request counter")
		}
		if !hasStatusCode {
			t.Error("Expected http.status_code attribute in request counter")
		}
	}
}

func verifyErrorCounterMetric(t *testing.T, m metricdata.Metrics) {
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Errorf("Expected Sum data for %s", m.Name)
		return
	}

	// Should have at least 2 data points (client error and server error)
	if len(sum.DataPoints) < 2 {
		t.Errorf("Expected at least 2 data points for %s, got %d", m.Name, len(sum.DataPoints))
		return
	}

	// Verify error type attributes are present
	errorTypes := make(map[string]bool)
	for _, dp := range sum.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "error.type" {
				errorTypes[attr.Value.AsString()] = true
			}
		}
	}

	expectedErrorTypes := []string{"client_error", "server_error"}
	for _, expectedType := range expectedErrorTypes {
		if !errorTypes[expectedType] {
			t.Errorf("Expected error type %s not found in error counter", expectedType)
		}
	}
}

func verifyDurationHistogramMetric(t *testing.T, m metricdata.Metrics) {
	histogram, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Errorf("Expected Histogram data for %s", m.Name)
		return
	}

	// Should have at least 3 data points (one for each request)
	if len(histogram.DataPoints) < 3 {
		t.Errorf("Expected at least 3 data points for %s, got %d", m.Name, len(histogram.DataPoints))
		return
	}

	// Verify all durations are positive
	for _, dp := range histogram.DataPoints {
		if dp.Count == 0 {
			t.Error("Expected non-zero count in duration histogram")
		}
		if dp.Sum <= 0 {
			t.Error("Expected positive sum in duration histogram")
		}
	}
}

func TestClientOTELMetricsLabeling(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Return different responses based on endpoint
		switch r.URL.Path {
		case "/v3/databases":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"databases": []}`))
		case "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"job": {"id": "123"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error": "Not found"}`))
		}
	}))
	defer server.Close()

	// Set up metrics
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	meter := mp.Meter("test")

	tp := trace.NewTracerProvider()
	tracer := tp.Tracer("test")

	// Create client
	client, err := NewClient("test-api-key",
		WithEndpoint(server.URL),
		WithOTEL(tracer, meter),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Make requests to different endpoints
	endpoints := []struct {
		path     string
		method   string
		expected map[string]string
	}{
		{
			path:   "/v3/databases",
			method: "GET",
			expected: map[string]string{
				"td.api_version": "v3",
				"td.endpoint":    "databases",
				"http.method":    "GET",
			},
		},
		{
			path:   "/v1/jobs",
			method: "POST",
			expected: map[string]string{
				"td.api_version": "v1",
				"td.endpoint":    "jobs",
				"http.method":    "POST",
			},
		},
	}

	for _, endpoint := range endpoints {
		req, err := client.NewRequest(endpoint.method, endpoint.path, nil)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		resp, err := client.Do(context.Background(), req, nil)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}

	// Collect and verify metrics have correct labels
	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	// Find request counter metric
	var requestCounter metricdata.Metrics
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "http_requests_total" {
				requestCounter = m
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	if !found {
		t.Fatal("Request counter metric not found")
	}

	sum, ok := requestCounter.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatal("Expected Sum data for request counter")
	}

	// Verify each endpoint has correct labels
	endpointLabels := make(map[string]map[string]string)
	for _, dp := range sum.DataPoints {
		attrs := make(map[string]string)
		for _, attr := range dp.Attributes.ToSlice() {
			attrs[string(attr.Key)] = attr.Value.AsString()
		}

		if endpoint, ok := attrs["td.endpoint"]; ok {
			endpointLabels[endpoint] = attrs
		}
	}

	for _, endpoint := range endpoints {
		expectedEndpoint := endpoint.expected["td.endpoint"]
		if labels, ok := endpointLabels[expectedEndpoint]; ok {
			for key, expectedValue := range endpoint.expected {
				if actualValue, exists := labels[key]; !exists {
					t.Errorf("Expected label %s not found for endpoint %s", key, expectedEndpoint)
				} else if actualValue != expectedValue {
					t.Errorf("Expected %s=%s for endpoint %s, got %s", key, expectedValue, expectedEndpoint, actualValue)
				}
			}
		} else {
			t.Errorf("Expected endpoint %s not found in metrics", expectedEndpoint)
		}
	}
}
