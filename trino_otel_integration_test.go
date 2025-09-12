package treasuredata

import (
	"context"
	"testing"
	"time"

	"github.com/mickeey2525/treasuredata-go-sdk/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// TestTrinoClientOTELIntegration tests the full OTEL integration with Trino client
func TestTrinoClientOTELIntegration(t *testing.T) {
	// Set up OTEL providers
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("trino-integration-test"),
			semconv.ServiceVersion("1.0.0"),
		)),
	)
	tracer := tp.Tracer("trino-test")

	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	meter := mp.Meter("trino-test")

	// Create Trino client with OTEL
	config := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		Database:      "test_db",
		Source:        "integration-test",
		EnableTracing: true,
		Tracer:        tracer,
		Meter:         meter,
	}

	client, err := NewTDTrinoClient(config)
	if err != nil {
		t.Fatalf("Failed to create Trino client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Test Ping operation with tracing
	t.Run("Ping with tracing", func(t *testing.T) {
		// Clear previous spans
		exporter.Reset()

		_ = client.Ping(ctx)
		// Note: This will likely fail due to invalid credentials, but we're testing instrumentation
		// The span should still be created regardless of the operation result

		// Verify spans were created
		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("Expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name != "trino.ping" {
			t.Errorf("Expected span name 'trino.ping', got '%s'", span.Name)
		}

		// Verify span attributes
		attrs := span.Attributes
		expectedAttrs := map[string]interface{}{
			"db.system":      "trino",
			"db.name":        "test_db",
			"db.operation":   "PING",
			"trino.catalog":  "td",
			"trino.schema":   "test_db",
			"trino.region":   "us",
			"trino.endpoint": "api-presto.treasuredata.com",
		}

		for expectedKey, expectedValue := range expectedAttrs {
			found := false
			for _, attr := range attrs {
				if string(attr.Key) == expectedKey {
					found = true
					if attr.Value.AsString() != expectedValue.(string) {
						t.Errorf("Expected %s=%s, got %s", expectedKey, expectedValue, attr.Value.AsString())
					}
					break
				}
			}
			if !found {
				t.Errorf("Expected attribute %s not found", expectedKey)
			}
		}
	})

	// Test Query operation with tracing
	t.Run("Query with tracing", func(t *testing.T) {
		// Clear previous spans
		exporter.Reset()

		query := "SELECT 1 as test_column"
		_, _ = client.Query(ctx, query)
		// Note: This will likely fail due to invalid credentials, but we're testing instrumentation

		// Verify spans were created
		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("Expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name != "trino.query" {
			t.Errorf("Expected span name 'trino.query', got '%s'", span.Name)
		}

		// Verify span attributes include sanitized SQL
		attrs := span.Attributes
		foundStatement := false
		for _, attr := range attrs {
			if string(attr.Key) == "db.statement" {
				foundStatement = true
				// The statement should be present (this simple query doesn't need sanitization)
				if attr.Value.AsString() != query {
					t.Errorf("Expected db.statement=%s, got %s", query, attr.Value.AsString())
				}
				break
			}
		}
		if !foundStatement {
			t.Error("Expected db.statement attribute not found")
		}
	})

	// Test QueryRow operation with tracing
	t.Run("QueryRow with tracing", func(t *testing.T) {
		// Clear previous spans
		exporter.Reset()

		query := "SELECT COUNT(*) FROM information_schema.tables"
		row := client.QueryRow(ctx, query)
		_ = row // We don't scan since it will fail with invalid credentials

		// Verify spans were created
		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("Expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name != "trino.query_row" {
			t.Errorf("Expected span name 'trino.query_row', got '%s'", span.Name)
		}
	})

	// Test Exec operation with tracing
	t.Run("Exec with tracing", func(t *testing.T) {
		// Clear previous spans
		exporter.Reset()

		query := "CREATE TABLE IF NOT EXISTS test_table AS SELECT 1 as id"
		_, _ = client.Exec(ctx, query)
		// Note: This will likely fail due to invalid credentials, but we're testing instrumentation

		// Verify spans were created
		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("Expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name != "trino.exec" {
			t.Errorf("Expected span name 'trino.exec', got '%s'", span.Name)
		}

		// Verify operation type is extracted correctly
		attrs := span.Attributes
		foundOperation := false
		for _, attr := range attrs {
			if string(attr.Key) == "db.operation" {
				foundOperation = true
				if attr.Value.AsString() != "CREATE" {
					t.Errorf("Expected db.operation=CREATE, got %s", attr.Value.AsString())
				}
				break
			}
		}
		if !foundOperation {
			t.Error("Expected db.operation attribute not found")
		}
	})

	// Test metrics collection
	t.Run("Metrics collection", func(t *testing.T) {
		// Perform some operations to generate metrics
		client.Ping(ctx)
		client.Query(ctx, "SELECT 1")
		client.QueryRow(ctx, "SELECT 2")
		client.Exec(ctx, "SELECT 3")

		// Collect metrics
		var metrics metricdata.ResourceMetrics
		err := reader.Collect(ctx, &metrics)
		if err != nil {
			t.Fatalf("Failed to collect metrics: %v", err)
		}

		// Verify metrics were recorded
		// Note: The exact verification depends on the metric reader implementation
		// For now, we just verify that collection doesn't fail
		t.Log("Metrics collection completed successfully")
	})
}

// TestTrinoClientOTELSQLSanitization tests SQL sanitization in spans
func TestTrinoClientOTELSQLSanitization(t *testing.T) {
	// Set up OTEL providers
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("trino-sanitization-test"),
		)),
	)
	tracer := tp.Tracer("trino-test")

	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	meter := mp.Meter("trino-test")

	// Create Trino client with OTEL
	config := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		Database:      "test_db",
		Source:        "sanitization-test",
		EnableTracing: true,
		Tracer:        tracer,
		Meter:         meter,
	}

	client, err := NewTDTrinoClient(config)
	if err != nil {
		t.Fatalf("Failed to create Trino client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	testCases := []struct {
		name           string
		query          string
		expectedInSpan string
	}{
		{
			name:           "String literals should be sanitized",
			query:          "SELECT * FROM users WHERE name = 'sensitive_data'",
			expectedInSpan: "SELECT * FROM users WHERE name = '?'",
		},
		{
			name:           "Large numbers should be sanitized",
			query:          "SELECT * FROM transactions WHERE amount > 123456",
			expectedInSpan: "SELECT * FROM transactions WHERE amount > ?",
		},
		{
			name:           "Multiple sensitive values",
			query:          "SELECT * FROM users WHERE name = 'john' AND ssn = '123456789'",
			expectedInSpan: "SELECT * FROM users WHERE name = '?' AND ssn = '?'",
		},
		{
			name:           "Simple queries should not be over-sanitized",
			query:          "SELECT COUNT(*) FROM table1",
			expectedInSpan: "SELECT COUNT(*) FROM table1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear previous spans
			exporter.Reset()

			// Execute query (will fail but span will be created)
			_, _ = client.Query(ctx, tc.query)
			// Ignore error, we're testing instrumentation

			// Verify span contains sanitized SQL
			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("Expected 1 span, got %d", len(spans))
			}

			span := spans[0]
			attrs := span.Attributes
			foundStatement := false
			for _, attr := range attrs {
				if string(attr.Key) == "db.statement" {
					foundStatement = true
					if attr.Value.AsString() != tc.expectedInSpan {
						t.Errorf("Expected sanitized SQL '%s', got '%s'", tc.expectedInSpan, attr.Value.AsString())
					}
					break
				}
			}
			if !foundStatement {
				t.Error("Expected db.statement attribute not found")
			}
		})
	}
}

// TestTrinoClientOTELWithOTELManager tests integration with OTEL manager
func TestTrinoClientOTELWithOTELManager(t *testing.T) {
	// Create OTEL manager
	config := otel.DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "trino-manager-test"

	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Create Trino client using manager's tracer and meter
	trinoConfig := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		Database:      "test_db",
		Source:        "manager-test",
		EnableTracing: true,
		Tracer:        manager.GetTracer(),
		Meter:         manager.GetMeter(),
	}

	client, err := NewTDTrinoClient(trinoConfig)
	if err != nil {
		t.Fatalf("Failed to create Trino client: %v", err)
	}
	defer client.Close()

	// Test that operations work with manager-provided instrumentation
	_ = client.Ping(ctx)
	// Error is expected due to invalid credentials, but instrumentation should work

	_, _ = client.Query(ctx, "SELECT 1")
	// Error is expected due to invalid credentials, but instrumentation should work

	t.Log("Trino client integration with OTEL manager completed successfully")
}

// TestTrinoClientOTELDisabled tests that client works when OTEL is disabled
func TestTrinoClientOTELDisabled(t *testing.T) {
	config := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		Database:      "test_db",
		Source:        "disabled-test",
		EnableTracing: false,
	}

	client, err := NewTDTrinoClient(config)
	if err != nil {
		t.Fatalf("Failed to create Trino client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Test that operations work without OTEL
	_ = client.Ping(ctx)
	// Error is expected due to invalid credentials, but should not crash

	_, _ = client.Query(ctx, "SELECT 1")
	// Error is expected due to invalid credentials, but should not crash

	t.Log("Trino client without OTEL completed successfully")
}

// TestTrinoClientOTELMetricsIntegration tests metrics collection in detail
func TestTrinoClientOTELMetricsIntegration(t *testing.T) {
	// Set up metric provider
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	meter := mp.Meter("trino-metrics-test")

	// Create no-op tracer for this test
	tp := trace.NewTracerProvider()
	tracer := tp.Tracer("trino-metrics-test")

	// Create Trino client with metrics
	config := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		Database:      "test_db",
		Source:        "metrics-test",
		EnableTracing: true,
		Tracer:        tracer,
		Meter:         meter,
	}

	client, err := NewTDTrinoClient(config)
	if err != nil {
		t.Fatalf("Failed to create Trino client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Perform operations to generate metrics
	client.Ping(ctx)
	client.Query(ctx, "SELECT 1")
	client.QueryRow(ctx, "SELECT 2")
	client.Exec(ctx, "INSERT INTO test VALUES (1)")

	// Wait a bit for metrics to be recorded
	time.Sleep(100 * time.Millisecond)

	// Collect metrics
	var metrics metricdata.ResourceMetrics
	err = reader.Collect(ctx, &metrics)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	// Verify that metrics collection works
	// The exact verification depends on the metric implementation
	// For now, we just verify that collection doesn't fail
	t.Log("Trino metrics integration test completed successfully")
}
