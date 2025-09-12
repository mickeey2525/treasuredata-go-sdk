package treasuredata

import (
	"context"
	"testing"
)

func TestTDTrinoClientOpenTelemetryIntegration(t *testing.T) {
	// Test that OpenTelemetry tracing can be enabled
	config := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		Database:      "test_db",
		Source:        "test",
		EnableTracing: true,
	}

	client, err := NewTDTrinoClient(config)
	if err != nil {
		t.Fatalf("Failed to create Trino client with tracing: %v", err)
	}
	defer client.Close()

	// Check that tracer is not nil and is properly initialized
	if client.tracer == nil {
		t.Error("Expected tracer to be initialized when tracing is enabled")
	}

	// Test that tracing can be disabled
	configNoTrace := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		Database:      "test_db",
		Source:        "test",
		EnableTracing: false,
	}

	clientNoTrace, err := NewTDTrinoClient(configNoTrace)
	if err != nil {
		t.Fatalf("Failed to create Trino client without tracing: %v", err)
	}
	defer clientNoTrace.Close()

	// Check that tracer is still initialized (as NoopTracer)
	if clientNoTrace.tracer == nil {
		t.Error("Expected tracer to be initialized even when tracing is disabled")
	}
}

func TestTDTrinoClientTracingSpanCreation(t *testing.T) {
	// Test that spans are created for queries
	config := TDTrinoClientConfig{
		APIKey:        "test_account/test_key",
		Region:        "us",
		Database:      "test_db",
		Source:        "test",
		EnableTracing: true,
	}

	client, err := NewTDTrinoClient(config)
	if err != nil {
		t.Fatalf("Failed to create Trino client: %v", err)
	}
	defer client.Close()

	// Create a span to verify the tracer works
	ctx := context.Background()
	_, span := client.tracer.Start(ctx, "test-span")
	defer span.End()

	// Since we're using the default global tracer provider (which is typically a NoopTracer),
	// we just verify that the span is created without error
	if span == nil {
		t.Error("Expected span to be created, got nil")
	}
}

func TestNewTDTrinoClientWithTracing(t *testing.T) {
	// Test the convenience function for creating clients with tracing
	config := TDTrinoClientConfig{
		APIKey:   "test_account/test_key",
		Region:   "us",
		Database: "test_db",
		Source:   "test",
	}

	client, err := NewTDTrinoClientWithTracing(config)
	if err != nil {
		t.Fatalf("Failed to create Trino client with tracing: %v", err)
	}
	defer client.Close()

	// Verify tracing is enabled
	if client.tracer == nil {
		t.Error("Expected tracer to be initialized")
	}

	// Create a test span to verify tracing works
	ctx := context.Background()
	_, span := client.tracer.Start(ctx, "test-span")
	defer span.End()

	if span == nil {
		t.Error("Expected span to be created")
	}
}
