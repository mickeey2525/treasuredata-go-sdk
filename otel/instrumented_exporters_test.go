package otel

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
)

// Mock trace exporter for testing
type mockTraceExporter struct {
	exportCalls   int
	shutdownCalls int
	shouldFail    bool
	failCount     int
}

func (m *mockTraceExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	m.exportCalls++
	if m.shouldFail && m.exportCalls <= m.failCount {
		return errors.New("mock export failure")
	}
	return nil
}

func (m *mockTraceExporter) Shutdown(ctx context.Context) error {
	m.shutdownCalls++
	if m.shouldFail {
		return errors.New("mock shutdown failure")
	}
	return nil
}

// Mock metric exporter for testing
type mockMetricExporter struct {
	exportCalls     int
	forceFlushCalls int
	shutdownCalls   int
	shouldFail      bool
	failCount       int
}

func (m *mockMetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	m.exportCalls++
	if m.shouldFail && m.exportCalls <= m.failCount {
		return errors.New("mock export failure")
	}
	return nil
}

func (m *mockMetricExporter) Temporality(kind metric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (m *mockMetricExporter) Aggregation(kind metric.InstrumentKind) metric.Aggregation {
	return metric.DefaultAggregationSelector(kind)
}

func (m *mockMetricExporter) ForceFlush(ctx context.Context) error {
	m.forceFlushCalls++
	if m.shouldFail {
		return errors.New("mock force flush failure")
	}
	return nil
}

func (m *mockMetricExporter) Shutdown(ctx context.Context) error {
	m.shutdownCalls++
	if m.shouldFail {
		return errors.New("mock shutdown failure")
	}
	return nil
}

func TestInstrumentedTraceExporter(t *testing.T) {
	mockExporter := &mockTraceExporter{}
	instrumentedExporter := NewInstrumentedTraceExporter(mockExporter, "test-service")

	ctx := context.Background()

	// Test successful export
	err := instrumentedExporter.ExportSpans(ctx, nil)
	if err != nil {
		t.Errorf("Expected successful export, got error: %v", err)
	}

	if mockExporter.exportCalls != 1 {
		t.Errorf("Expected 1 export call, got %d", mockExporter.exportCalls)
	}

	// Test export with retry
	mockExporter.shouldFail = true
	mockExporter.failCount = 1
	mockExporter.exportCalls = 0

	err = instrumentedExporter.ExportSpans(ctx, nil)
	if err != nil {
		t.Errorf("Expected successful export after retry, got error: %v", err)
	}

	if mockExporter.exportCalls != 2 {
		t.Errorf("Expected 2 export calls (1 failure + 1 success), got %d", mockExporter.exportCalls)
	}

	// Test shutdown
	mockExporter.shouldFail = false
	err = instrumentedExporter.Shutdown(ctx)
	if err != nil {
		t.Errorf("Expected successful shutdown, got error: %v", err)
	}

	if mockExporter.shutdownCalls != 1 {
		t.Errorf("Expected 1 shutdown call, got %d", mockExporter.shutdownCalls)
	}

	// Test stats
	stats := instrumentedExporter.GetStats()
	if stats["exporter_type"] != "trace" {
		t.Errorf("Expected exporter_type to be 'trace', got %v", stats["exporter_type"])
	}

	if stats["service_name"] != "test-service" {
		t.Errorf("Expected service_name to be 'test-service', got %v", stats["service_name"])
	}
}

func TestInstrumentedMetricExporter(t *testing.T) {
	mockExporter := &mockMetricExporter{}
	instrumentedExporter := NewInstrumentedMetricExporter(mockExporter, "test-service")

	ctx := context.Background()

	// Test successful export
	err := instrumentedExporter.Export(ctx, nil)
	if err != nil {
		t.Errorf("Expected successful export, got error: %v", err)
	}

	if mockExporter.exportCalls != 1 {
		t.Errorf("Expected 1 export call, got %d", mockExporter.exportCalls)
	}

	// Test force flush
	err = instrumentedExporter.ForceFlush(ctx)
	if err != nil {
		t.Errorf("Expected successful force flush, got error: %v", err)
	}

	if mockExporter.forceFlushCalls != 1 {
		t.Errorf("Expected 1 force flush call, got %d", mockExporter.forceFlushCalls)
	}

	// Test export with retry
	mockExporter.shouldFail = true
	mockExporter.failCount = 1
	mockExporter.exportCalls = 0

	err = instrumentedExporter.Export(ctx, nil)
	if err != nil {
		t.Errorf("Expected successful export after retry, got error: %v", err)
	}

	if mockExporter.exportCalls != 2 {
		t.Errorf("Expected 2 export calls (1 failure + 1 success), got %d", mockExporter.exportCalls)
	}

	// Test shutdown
	mockExporter.shouldFail = false
	err = instrumentedExporter.Shutdown(ctx)
	if err != nil {
		t.Errorf("Expected successful shutdown, got error: %v", err)
	}

	if mockExporter.shutdownCalls != 1 {
		t.Errorf("Expected 1 shutdown call, got %d", mockExporter.shutdownCalls)
	}

	// Test stats
	stats := instrumentedExporter.GetStats()
	if stats["exporter_type"] != "metric" {
		t.Errorf("Expected exporter_type to be 'metric', got %v", stats["exporter_type"])
	}

	if stats["service_name"] != "test-service" {
		t.Errorf("Expected service_name to be 'test-service', got %v", stats["service_name"])
	}
}

func TestInstrumentedExporterCircuitBreaker(t *testing.T) {
	mockExporter := &mockTraceExporter{
		shouldFail: true,
		failCount:  10, // Always fail
	}

	// Create exporter with custom config that has lower failure threshold
	config := DefaultOTELConfig()
	config.CircuitFailureThreshold = 3
	config.MaxRetryAttempts = 1 // Reduce retry attempts to speed up test
	config.RetryDelay = 10 * time.Millisecond

	instrumentedExporter := NewInstrumentedTraceExporterWithConfig(mockExporter, "test-service", config)

	ctx := context.Background()

	// Trigger circuit breaker by causing multiple failures
	for i := 0; i < 4; i++ {
		instrumentedExporter.ExportSpans(ctx, nil)
	}

	// Check that circuit breaker is now open
	stats := instrumentedExporter.GetStats()
	if stats["circuit_breaker_state"] != "OPEN" {
		t.Errorf("Expected circuit breaker to be OPEN, got %s", stats["circuit_breaker_state"])
	}

	// Verify that subsequent calls are rejected without calling the underlying exporter
	initialCalls := mockExporter.exportCalls
	err := instrumentedExporter.ExportSpans(ctx, nil)
	if err == nil {
		t.Error("Expected error when circuit breaker is open")
	}

	if mockExporter.exportCalls != initialCalls {
		t.Error("Expected no additional calls to underlying exporter when circuit is open")
	}
}

func TestExporterManager(t *testing.T) {
	manager := NewExporterManager()

	// Test initial state
	if manager.GetTraceExporter() != nil {
		t.Error("Expected trace exporter to be nil initially")
	}

	if manager.GetMetricExporter() != nil {
		t.Error("Expected metric exporter to be nil initially")
	}

	// Set exporters
	mockTraceExporter := &mockTraceExporter{}
	instrumentedTraceExporter := NewInstrumentedTraceExporter(mockTraceExporter, "test-service")
	manager.SetTraceExporter(instrumentedTraceExporter)

	mockMetricExporter := &mockMetricExporter{}
	instrumentedMetricExporter := NewInstrumentedMetricExporter(mockMetricExporter, "test-service")
	manager.SetMetricExporter(instrumentedMetricExporter)

	// Test getters
	if manager.GetTraceExporter() != instrumentedTraceExporter {
		t.Error("Expected trace exporter to be set")
	}

	if manager.GetMetricExporter() != instrumentedMetricExporter {
		t.Error("Expected metric exporter to be set")
	}

	// Test stats
	stats := manager.GetAllStats()
	if stats["trace"] == nil {
		t.Error("Expected trace stats to be present")
	}

	if stats["metric"] == nil {
		t.Error("Expected metric stats to be present")
	}

	// Test reset
	manager.Reset()

	// Verify reset worked by checking that failure handlers were reset
	traceStats := manager.GetTraceExporter().GetStats()
	if traceStats["consecutive_failures"].(int) != 0 {
		t.Error("Expected consecutive failures to be reset to 0")
	}

	metricStats := manager.GetMetricExporter().GetStats()
	if metricStats["consecutive_failures"].(int) != 0 {
		t.Error("Expected consecutive failures to be reset to 0")
	}
}

func TestExporterManagerWithNilExporters(t *testing.T) {
	manager := NewExporterManager()

	// Test stats with nil exporters
	stats := manager.GetAllStats()
	if len(stats) != 0 {
		t.Errorf("Expected empty stats with nil exporters, got %v", stats)
	}

	// Test reset with nil exporters (should not panic)
	manager.Reset()

	// Test log stats with nil exporters (should not panic)
	manager.LogStats()
}
