package otel

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// TestGracefulDegradationDisabledOTEL tests behavior when OTEL is disabled
func TestGracefulDegradationDisabledOTEL(t *testing.T) {
	// Test with disabled OTEL
	config := DefaultOTELConfig()
	config.Enabled = false
	config.ServiceName = "disabled-test"

	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Should get no-op implementations
	tracer := manager.GetTracer()
	if tracer == nil {
		t.Error("Expected tracer to be available even when disabled")
	}

	meter := manager.GetMeter()
	if meter == nil {
		t.Error("Expected meter to be available even when disabled")
	}

	// Test operations work without errors
	testBasicOperations(t, tracer, meter, "disabled")
}

// TestGracefulDegradationNoEndpoints tests behavior when no export endpoints are configured
func TestGracefulDegradationNoEndpoints(t *testing.T) {
	// Test with enabled OTEL but no endpoints
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "no-endpoints-test"
	// Don't set TraceEndpoint or MetricEndpoint

	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	tracer := manager.GetTracer()
	if tracer == nil {
		t.Error("Expected tracer to be available")
	}

	meter := manager.GetMeter()
	if meter == nil {
		t.Error("Expected meter to be available")
	}

	// Test operations work without errors
	testBasicOperations(t, tracer, meter, "no-endpoints")

	// Verify manager is still functional
	if !manager.IsInitialized() {
		t.Error("Manager should be initialized")
	}

	if !manager.IsEnabled() {
		t.Error("Manager should be enabled")
	}
}

// TestGracefulDegradationInvalidSamplingRate tests behavior with invalid sampling rate
func TestGracefulDegradationInvalidSamplingRate(t *testing.T) {
	// Test with invalid sampling rate
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "invalid-sampling-test"
	config.SamplingRate = 2.0 // Invalid: > 1.0

	_, err := NewOTELManager(config)
	if err == nil {
		t.Error("Expected error with invalid sampling rate")
	}

	// Should be able to create with valid config as fallback
	validConfig := DefaultOTELConfig()
	validConfig.Enabled = true
	validConfig.ServiceName = "valid-sampling-test"
	validConfig.SamplingRate = 0.5

	manager, err := NewOTELManager(validConfig)
	if err != nil {
		t.Fatalf("Failed to create manager with valid config: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Test operations work
	tracer := manager.GetTracer()
	meter := manager.GetMeter()
	testBasicOperations(t, tracer, meter, "valid-sampling")
}

// TestGracefulDegradationEmptyServiceName tests behavior with empty service name
func TestGracefulDegradationEmptyServiceName(t *testing.T) {
	// Test with empty service name
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "" // Invalid: empty

	_, err := NewOTELManager(config)
	if err == nil {
		t.Error("Expected error with empty service name")
	}

	// Should be able to create with valid config as fallback
	validConfig := DefaultOTELConfig()
	validConfig.Enabled = true
	validConfig.ServiceName = "valid-service"

	manager, err := NewOTELManager(validConfig)
	if err != nil {
		t.Fatalf("Failed to create manager with valid config: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Test operations work
	tracer := manager.GetTracer()
	meter := manager.GetMeter()
	testBasicOperations(t, tracer, meter, "valid-service")
}

// TestGracefulDegradationHighLoad tests behavior under high load
func TestGracefulDegradationHighLoad(t *testing.T) {
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "high-load-test"
	config.BatchTimeout = 10 * time.Millisecond // Very short for stress test
	config.BatchSize = 10

	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	tracer := manager.GetTracer()
	meter := manager.GetMeter()

	// Generate high load
	const numOperations = 1000
	start := time.Now()

	for i := 0; i < numOperations; i++ {
		_, span := tracer.Start(ctx, "high-load-span")
		span.SetAttributes(
			attribute.Int("iteration", i),
			attribute.String("test", "high-load"),
		)
		span.End()

		counter, _ := meter.Int64Counter("high_load_counter")
		counter.Add(ctx, 1, metric.WithAttributes(
			attribute.Int("iteration", i),
		))
	}

	duration := time.Since(start)
	t.Logf("High load test: %d operations in %v", numOperations, duration)

	// Should complete in reasonable time
	if duration > 10*time.Second {
		t.Errorf("High load test took too long: %v", duration)
	}

	// Test that operations still work after high load
	testBasicOperations(t, tracer, meter, "post-high-load")
}

// TestGracefulDegradationConcurrentAccess tests concurrent access to OTEL components
func TestGracefulDegradationConcurrentAccess(t *testing.T) {
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "concurrent-test"

	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	tracer := manager.GetTracer()
	meter := manager.GetMeter()

	// Test concurrent access
	const numGoroutines = 10
	const operationsPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer func() { done <- true }()

			for j := 0; j < operationsPerGoroutine; j++ {
				// Create spans concurrently
				_, span := tracer.Start(ctx, "concurrent-span")
				span.SetAttributes(
					attribute.Int("worker_id", workerID),
					attribute.Int("operation_id", j),
				)
				span.End()

				// Record metrics concurrently
				counter, _ := meter.Int64Counter("concurrent_counter")
				counter.Add(ctx, 1, metric.WithAttributes(
					attribute.Int("worker_id", workerID),
				))
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Test that operations still work after concurrent access
	testBasicOperations(t, tracer, meter, "post-concurrent")
}

// TestGracefulDegradationShutdownDuringOperations tests shutdown during active operations
func TestGracefulDegradationShutdownDuringOperations(t *testing.T) {
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "shutdown-test"

	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}

	tracer := manager.GetTracer()
	meter := manager.GetMeter()

	// Start operations in background
	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		for i := 0; i < 100; i++ {
			_, span := tracer.Start(ctx, "shutdown-test-span")
			span.SetAttributes(attribute.Int("iteration", i))
			span.End()

			counter, _ := meter.Int64Counter("shutdown_test_counter")
			counter.Add(ctx, 1)

			time.Sleep(time.Millisecond)
		}
	}()

	// Shutdown while operations are running
	time.Sleep(50 * time.Millisecond)
	err = manager.Shutdown(ctx)
	if err != nil {
		t.Errorf("Shutdown should not fail: %v", err)
	}

	// Wait for background operations to complete
	<-done

	// Manager should be shutdown
	if manager.IsInitialized() {
		t.Error("Manager should not be initialized after shutdown")
	}
}

// testBasicOperations tests basic OTEL operations
func testBasicOperations(t *testing.T, tracer trace.Tracer, meter metric.Meter, testName string) {
	ctx := context.Background()

	// Test span creation
	_, span := tracer.Start(ctx, testName+"-span")
	if span == nil {
		t.Errorf("Expected span to be created for %s", testName)
		return
	}

	span.SetAttributes(
		attribute.String("test.name", testName),
		attribute.Bool("test.working", true),
	)
	span.SetStatus(codes.Ok, "Test completed")
	span.End()

	// Test nested spans
	ctx, parentSpan := tracer.Start(ctx, testName+"-parent")
	_, childSpan := tracer.Start(ctx, testName+"-child")
	childSpan.End()
	parentSpan.End()

	// Test metric creation and recording
	counter, err := meter.Int64Counter(testName + "_counter")
	if err != nil {
		t.Errorf("Failed to create counter for %s: %v", testName, err)
		return
	}

	counter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("test.name", testName),
	))

	histogram, err := meter.Float64Histogram(testName + "_histogram")
	if err != nil {
		t.Errorf("Failed to create histogram for %s: %v", testName, err)
		return
	}

	histogram.Record(ctx, 1.0, metric.WithAttributes(
		attribute.String("test.name", testName),
	))

	gauge, err := meter.Int64UpDownCounter(testName + "_gauge")
	if err != nil {
		t.Errorf("Failed to create gauge for %s: %v", testName, err)
		return
	}

	gauge.Add(ctx, 1, metric.WithAttributes(
		attribute.String("test.name", testName),
	))

	t.Logf("Basic operations test passed for %s", testName)
}
