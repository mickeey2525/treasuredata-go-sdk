package otel

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// BenchmarkResult holds performance benchmark results
type BenchmarkResult struct {
	Name           string
	Duration       time.Duration
	AllocBytes     uint64
	AllocObjects   uint64
	MemoryUsage    uint64
	GoroutineCount int
}

// PerformanceTestSuite provides utilities for performance testing
type PerformanceTestSuite struct {
	t               *testing.T
	enabledManager  *OTELManager
	disabledManager *OTELManager
	tracer          trace.Tracer
	meter           metric.Meter
	noopTracer      trace.Tracer
	noopMeter       metric.Meter
}

// NewPerformanceTestSuite creates a new performance test suite
func NewPerformanceTestSuite(t *testing.T) *PerformanceTestSuite {
	// Create enabled OTEL manager
	enabledConfig := DefaultOTELConfig()
	enabledConfig.Enabled = true
	enabledConfig.ServiceName = "performance-test"
	enabledConfig.BatchTimeout = 100 * time.Millisecond
	enabledConfig.BatchSize = 100

	enabledManager, err := NewOTELManager(enabledConfig)
	if err != nil {
		t.Fatalf("Failed to create enabled OTEL manager: %v", err)
	}

	err = enabledManager.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize enabled OTEL manager: %v", err)
	}

	// Create disabled OTEL manager
	disabledConfig := DefaultOTELConfig()
	disabledConfig.Enabled = false

	disabledManager, err := NewOTELManager(disabledConfig)
	if err != nil {
		t.Fatalf("Failed to create disabled OTEL manager: %v", err)
	}

	err = disabledManager.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize disabled OTEL manager: %v", err)
	}

	return &PerformanceTestSuite{
		t:               t,
		enabledManager:  enabledManager,
		disabledManager: disabledManager,
		tracer:          enabledManager.GetTracer(),
		meter:           enabledManager.GetMeter(),
		noopTracer:      disabledManager.GetTracer(),
		noopMeter:       disabledManager.GetMeter(),
	}
}

// Cleanup cleans up test resources
func (pts *PerformanceTestSuite) Cleanup() {
	ctx := context.Background()
	if pts.enabledManager != nil {
		pts.enabledManager.Shutdown(ctx)
	}
	if pts.disabledManager != nil {
		pts.disabledManager.Shutdown(ctx)
	}
}

// measurePerformance measures the performance of a function
func (pts *PerformanceTestSuite) measurePerformance(name string, iterations int, fn func()) BenchmarkResult {
	runtime.GC()

	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	goroutinesBefore := runtime.NumGoroutine()

	start := time.Now()
	for i := 0; i < iterations; i++ {
		fn()
	}
	duration := time.Since(start)

	runtime.ReadMemStats(&memAfter)
	goroutinesAfter := runtime.NumGoroutine()

	return BenchmarkResult{
		Name:           name,
		Duration:       duration,
		AllocBytes:     memAfter.TotalAlloc - memBefore.TotalAlloc,
		AllocObjects:   memAfter.Mallocs - memBefore.Mallocs,
		MemoryUsage:    memAfter.Sys - memBefore.Sys,
		GoroutineCount: goroutinesAfter - goroutinesBefore,
	}
}

// BenchmarkSpanCreation benchmarks span creation with and without OTEL
func BenchmarkSpanCreation(b *testing.B) {
	suite := NewPerformanceTestSuite(&testing.T{})
	defer suite.Cleanup()

	b.Run("WithOTEL", func(b *testing.B) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, span := suite.tracer.Start(ctx, "test-span")
			span.SetAttributes(
				attribute.String("operation", "test"),
				attribute.Int("iteration", i),
			)
			span.End()
		}
	})

	b.Run("WithoutOTEL", func(b *testing.B) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, span := suite.noopTracer.Start(ctx, "test-span")
			span.SetAttributes(
				attribute.String("operation", "test"),
				attribute.Int("iteration", i),
			)
			span.End()
		}
	})
}

// BenchmarkMetricRecording benchmarks metric recording with and without OTEL
func BenchmarkMetricRecording(b *testing.B) {
	suite := NewPerformanceTestSuite(&testing.T{})
	defer suite.Cleanup()

	b.Run("WithOTEL", func(b *testing.B) {
		ctx := context.Background()
		counter, _ := suite.meter.Int64Counter("test_counter")
		histogram, _ := suite.meter.Float64Histogram("test_histogram")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			counter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("operation", "test"),
			))
			histogram.Record(ctx, float64(i), metric.WithAttributes(
				attribute.String("operation", "test"),
			))
		}
	})

	b.Run("WithoutOTEL", func(b *testing.B) {
		ctx := context.Background()
		counter, _ := suite.noopMeter.Int64Counter("test_counter")
		histogram, _ := suite.noopMeter.Float64Histogram("test_histogram")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			counter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("operation", "test"),
			))
			histogram.Record(ctx, float64(i), metric.WithAttributes(
				attribute.String("operation", "test"),
			))
		}
	})
}

// BenchmarkConcurrentSpanCreation benchmarks concurrent span creation
func BenchmarkConcurrentSpanCreation(b *testing.B) {
	suite := NewPerformanceTestSuite(&testing.T{})
	defer suite.Cleanup()

	b.Run("WithOTEL", func(b *testing.B) {
		ctx := context.Background()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				_, span := suite.tracer.Start(ctx, "concurrent-span")
				span.SetAttributes(
					attribute.String("worker", fmt.Sprintf("worker-%d", i%10)),
					attribute.Int("iteration", i),
				)
				span.End()
				i++
			}
		})
	})

	b.Run("WithoutOTEL", func(b *testing.B) {
		ctx := context.Background()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				_, span := suite.noopTracer.Start(ctx, "concurrent-span")
				span.SetAttributes(
					attribute.String("worker", fmt.Sprintf("worker-%d", i%10)),
					attribute.Int("iteration", i),
				)
				span.End()
				i++
			}
		})
	})
}

// TestPerformanceComparison compares performance with and without OTEL
func TestPerformanceComparison(t *testing.T) {
	suite := NewPerformanceTestSuite(t)
	defer suite.Cleanup()

	iterations := 10000

	// Test span creation performance
	t.Run("SpanCreationComparison", func(t *testing.T) {
		withOTEL := suite.measurePerformance("WithOTEL", iterations, func() {
			ctx := context.Background()
			_, span := suite.tracer.Start(ctx, "test-span")
			span.SetAttributes(attribute.String("test", "value"))
			span.End()
		})

		withoutOTEL := suite.measurePerformance("WithoutOTEL", iterations, func() {
			ctx := context.Background()
			_, span := suite.noopTracer.Start(ctx, "test-span")
			span.SetAttributes(attribute.String("test", "value"))
			span.End()
		})

		t.Logf("Span Creation Performance:")
		t.Logf("  With OTEL:    %v (%d bytes, %d objects)", withOTEL.Duration, withOTEL.AllocBytes, withOTEL.AllocObjects)
		t.Logf("  Without OTEL: %v (%d bytes, %d objects)", withoutOTEL.Duration, withoutOTEL.AllocBytes, withoutOTEL.AllocObjects)

		// Performance should not degrade more than 10x when OTEL is enabled
		if withOTEL.Duration > withoutOTEL.Duration*10 {
			t.Errorf("OTEL span creation is too slow: %v vs %v (>10x degradation)", withOTEL.Duration, withoutOTEL.Duration)
		}

		// Memory usage should be reasonable
		if withOTEL.AllocBytes > withoutOTEL.AllocBytes*100 {
			t.Errorf("OTEL span creation uses too much memory: %d vs %d bytes (>100x increase)", withOTEL.AllocBytes, withoutOTEL.AllocBytes)
		}
	})

	// Test metric recording performance
	t.Run("MetricRecordingComparison", func(t *testing.T) {
		withOTEL := suite.measurePerformance("WithOTEL", iterations, func() {
			ctx := context.Background()
			counter, _ := suite.meter.Int64Counter("test_counter")
			counter.Add(ctx, 1, metric.WithAttributes(attribute.String("test", "value")))
		})

		withoutOTEL := suite.measurePerformance("WithoutOTEL", iterations, func() {
			ctx := context.Background()
			counter, _ := suite.noopMeter.Int64Counter("test_counter")
			counter.Add(ctx, 1, metric.WithAttributes(attribute.String("test", "value")))
		})

		t.Logf("Metric Recording Performance:")
		t.Logf("  With OTEL:    %v (%d bytes, %d objects)", withOTEL.Duration, withOTEL.AllocBytes, withOTEL.AllocObjects)
		t.Logf("  Without OTEL: %v (%d bytes, %d objects)", withoutOTEL.Duration, withoutOTEL.AllocBytes, withoutOTEL.AllocObjects)

		// Performance should not degrade more than 10x when OTEL is enabled
		if withOTEL.Duration > withoutOTEL.Duration*10 {
			t.Errorf("OTEL metric recording is too slow: %v vs %v (>10x degradation)", withOTEL.Duration, withoutOTEL.Duration)
		}
	})
}

// TestMemoryUsageUnderLoad tests memory usage under sustained load
func TestMemoryUsageUnderLoad(t *testing.T) {
	suite := NewPerformanceTestSuite(t)
	defer suite.Cleanup()

	ctx := context.Background()

	// Measure baseline memory
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Create sustained load
	const numWorkers = 10
	const operationsPerWorker = 1000

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			counter, _ := suite.meter.Int64Counter("load_test_counter")
			histogram, _ := suite.meter.Float64Histogram("load_test_histogram")

			for j := 0; j < operationsPerWorker; j++ {
				// Create span
				_, span := suite.tracer.Start(ctx, "load-test-span")
				span.SetAttributes(
					attribute.Int("worker_id", workerID),
					attribute.Int("operation_id", j),
					attribute.String("test_type", "load"),
				)

				// Record metrics
				counter.Add(ctx, 1, metric.WithAttributes(
					attribute.Int("worker_id", workerID),
				))
				histogram.Record(ctx, float64(j), metric.WithAttributes(
					attribute.Int("worker_id", workerID),
				))

				// Simulate some work
				time.Sleep(time.Microsecond)

				span.End()
			}
		}(i)
	}

	wg.Wait()

	// Force garbage collection and measure memory
	runtime.GC()
	time.Sleep(100 * time.Millisecond) // Allow GC to complete
	runtime.GC()

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	memoryIncrease := memAfter.Sys - memBefore.Sys
	t.Logf("Memory usage under load:")
	t.Logf("  Before: %d bytes", memBefore.Sys)
	t.Logf("  After:  %d bytes", memAfter.Sys)
	t.Logf("  Increase: %d bytes", memoryIncrease)

	// Memory increase should be reasonable (less than 50MB for this test)
	const maxMemoryIncrease = 50 * 1024 * 1024 // 50MB
	if memoryIncrease > maxMemoryIncrease {
		t.Errorf("Memory usage increased too much: %d bytes (max allowed: %d)", memoryIncrease, maxMemoryIncrease)
	}
}

// TestGracefulDegradationMissingDependencies tests behavior when OTEL dependencies are missing
func TestGracefulDegradationMissingDependencies(t *testing.T) {
	// Test with disabled OTEL (simulates missing dependencies)
	config := DefaultOTELConfig()
	config.Enabled = false // Disabled to simulate missing dependencies
	config.ServiceName = "test-service"

	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	// Initialize should succeed with disabled OTEL
	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize should not fail with disabled OTEL: %v", err)
	}

	// Should still be able to get tracer and meter (no-op implementations)
	tracer := manager.GetTracer()
	if tracer == nil {
		t.Error("Expected tracer to be available even when disabled")
	}

	meter := manager.GetMeter()
	if meter == nil {
		t.Error("Expected meter to be available even when disabled")
	}

	// Should be able to create spans and metrics without errors
	_, span := tracer.Start(ctx, "test-span")
	span.SetAttributes(attribute.String("test", "value"))
	span.End()

	counter, err := meter.Int64Counter("test_counter")
	if err != nil {
		t.Errorf("Failed to create counter: %v", err)
	} else {
		counter.Add(ctx, 1)
	}

	// Cleanup should not fail
	err = manager.Shutdown(ctx)
	if err != nil {
		t.Errorf("Shutdown should not fail: %v", err)
	}
}

// TestGracefulDegradationInvalidConfiguration tests behavior with invalid configuration
func TestGracefulDegradationInvalidConfiguration(t *testing.T) {
	testCases := []struct {
		name   string
		config *OTELConfig
	}{
		{
			name: "invalid sampling rate",
			config: &OTELConfig{
				Enabled:      true,
				ServiceName:  "test",
				SamplingRate: 2.0, // Invalid: > 1.0
			},
		},
		{
			name: "empty service name",
			config: &OTELConfig{
				Enabled:     true,
				ServiceName: "", // Invalid: empty
			},
		},
		{
			name: "invalid endpoint",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test",
				TraceEndpoint: "invalid-url", // Invalid: not a valid URL
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewOTELManager(tc.config)
			if err == nil {
				t.Error("Expected error with invalid configuration")
			}

			// Should be able to create manager with default config as fallback
			defaultConfig := DefaultOTELConfig()
			manager, err := NewOTELManager(defaultConfig)
			if err != nil {
				t.Fatalf("Failed to create manager with default config: %v", err)
			}

			ctx := context.Background()
			err = manager.Initialize(ctx)
			if err != nil {
				t.Fatalf("Failed to initialize with default config: %v", err)
			}

			manager.Shutdown(ctx)
		})
	}
}

// TestGracefulDegradationExportFailures tests behavior when exports fail
func TestGracefulDegradationExportFailures(t *testing.T) {
	// Test with enabled OTEL but no endpoints (will use no-op exporters)
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "test-service"
	// Don't set endpoints - this will create providers without exporters
	config.BatchTimeout = 50 * time.Millisecond
	config.BatchSize = 10
	config.ExportTimeout = 100 * time.Millisecond

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

	// Create spans and metrics - should work without exporters
	for i := 0; i < 20; i++ {
		_, span := tracer.Start(ctx, "no-export-span")
		span.SetAttributes(
			attribute.Int("iteration", i),
			attribute.String("test", "no-export"),
		)
		if i%5 == 0 {
			span.SetStatus(codes.Error, "simulated error")
		}
		span.End()

		counter, _ := meter.Int64Counter("no_export_counter")
		counter.Add(ctx, 1, metric.WithAttributes(
			attribute.Int("iteration", i),
		))

		// Small delay
		time.Sleep(5 * time.Millisecond)
	}

	// Application should continue working
	_, span := tracer.Start(ctx, "post-no-export-span")
	span.SetAttributes(attribute.String("status", "working"))
	span.End()

	counter, err := meter.Int64Counter("post_no_export_counter")
	if err != nil {
		t.Errorf("Failed to create counter: %v", err)
	} else {
		counter.Add(ctx, 1)
	}

	// Check export stats
	stats := manager.GetExportStats()
	t.Logf("Export stats: %+v", stats)

	if !stats["initialized"].(bool) {
		t.Error("Manager should be initialized")
	}
}

// TestLatencyImpact measures the latency impact of OTEL instrumentation
func TestLatencyImpact(t *testing.T) {
	suite := NewPerformanceTestSuite(t)
	defer suite.Cleanup()

	// Simulate a typical operation with nested spans
	simulateOperation := func(tracer trace.Tracer, meter metric.Meter, withInstrumentation bool) time.Duration {
		ctx := context.Background()
		start := time.Now()

		if withInstrumentation {
			// Main operation span
			ctx, mainSpan := tracer.Start(ctx, "main-operation")
			mainSpan.SetAttributes(
				attribute.String("operation.type", "database"),
				attribute.String("database.name", "test_db"),
			)

			// Nested operation 1
			_, span1 := tracer.Start(ctx, "query-preparation")
			span1.SetAttributes(attribute.String("query.type", "SELECT"))
			time.Sleep(100 * time.Microsecond) // Simulate work
			span1.End()

			// Nested operation 2
			_, span2 := tracer.Start(ctx, "query-execution")
			span2.SetAttributes(
				attribute.String("query.statement", "SELECT * FROM table"),
				attribute.Int("query.rows", 100),
			)
			time.Sleep(200 * time.Microsecond) // Simulate work
			span2.End()

			// Record metrics
			counter, _ := meter.Int64Counter("operations_total")
			counter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("operation.type", "database"),
			))

			histogram, _ := meter.Float64Histogram("operation_duration")
			histogram.Record(ctx, 0.3, metric.WithAttributes(
				attribute.String("operation.type", "database"),
			))

			mainSpan.End()
		} else {
			// Same work without instrumentation
			time.Sleep(100 * time.Microsecond)
			time.Sleep(200 * time.Microsecond)
		}

		return time.Since(start)
	}

	const iterations = 1000
	var withOTELTotal, withoutOTELTotal time.Duration

	// Measure with OTEL
	for i := 0; i < iterations; i++ {
		duration := simulateOperation(suite.tracer, suite.meter, true)
		withOTELTotal += duration
	}

	// Measure without OTEL
	for i := 0; i < iterations; i++ {
		duration := simulateOperation(suite.noopTracer, suite.noopMeter, false)
		withoutOTELTotal += duration
	}

	avgWithOTEL := withOTELTotal / iterations
	avgWithoutOTEL := withoutOTELTotal / iterations
	overhead := avgWithOTEL - avgWithoutOTEL
	overheadPercent := float64(overhead) / float64(avgWithoutOTEL) * 100

	t.Logf("Latency Impact Analysis:")
	t.Logf("  Average with OTEL:    %v", avgWithOTEL)
	t.Logf("  Average without OTEL: %v", avgWithoutOTEL)
	t.Logf("  Overhead:             %v (%.2f%%)", overhead, overheadPercent)

	// Overhead should be reasonable (less than 50% for this test)
	if overheadPercent > 50 {
		t.Errorf("OTEL overhead is too high: %.2f%% (max allowed: 50%%)", overheadPercent)
	}

	// Absolute overhead should be reasonable (less than 1ms for this test)
	if overhead > time.Millisecond {
		t.Errorf("OTEL absolute overhead is too high: %v (max allowed: 1ms)", overhead)
	}
}

// TestResourceLeakDetection tests for resource leaks
func TestResourceLeakDetection(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	// Create and destroy multiple managers
	for i := 0; i < 10; i++ {
		config := DefaultOTELConfig()
		config.Enabled = true
		config.ServiceName = fmt.Sprintf("leak-test-%d", i)

		manager, err := NewOTELManager(config)
		if err != nil {
			t.Fatalf("Failed to create manager %d: %v", i, err)
		}

		ctx := context.Background()
		err = manager.Initialize(ctx)
		if err != nil {
			t.Fatalf("Failed to initialize manager %d: %v", i, err)
		}

		// Use the manager briefly
		tracer := manager.GetTracer()
		_, span := tracer.Start(ctx, "leak-test-span")
		span.End()

		meter := manager.GetMeter()
		counter, _ := meter.Int64Counter("leak_test_counter")
		counter.Add(ctx, 1)

		// Shutdown
		err = manager.Shutdown(ctx)
		if err != nil {
			t.Fatalf("Failed to shutdown manager %d: %v", i, err)
		}
	}

	// Force garbage collection
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()
	goroutineLeak := finalGoroutines - initialGoroutines

	t.Logf("Goroutine count: initial=%d, final=%d, leak=%d",
		initialGoroutines, finalGoroutines, goroutineLeak)

	// Allow for some variance in goroutine count, but detect significant leaks
	if goroutineLeak > 5 {
		t.Errorf("Potential goroutine leak detected: %d extra goroutines", goroutineLeak)
	}
}
