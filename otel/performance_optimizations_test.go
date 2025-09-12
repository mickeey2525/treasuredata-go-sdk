package otel

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// TestAttributePool tests the attribute pool functionality
func TestAttributePool(t *testing.T) {
	pool := NewAttributePool()

	// Test basic get/put operations
	attrs1 := pool.Get()
	if attrs1 == nil {
		t.Fatal("Expected non-nil attribute slice")
	}

	attrs1 = append(attrs1, attribute.String("key1", "value1"))
	attrs1 = append(attrs1, attribute.String("key2", "value2"))

	pool.Put(attrs1)

	// Get another slice - should be reused
	attrs2 := pool.Get()
	if len(attrs2) != 0 {
		t.Errorf("Expected empty slice after put, got length %d", len(attrs2))
	}

	// Verify capacity is preserved
	if cap(attrs2) < 2 {
		t.Errorf("Expected capacity >= 2, got %d", cap(attrs2))
	}

	pool.Put(attrs2)
}

// TestOptimizedSpanCreator tests the optimized span creator
func TestOptimizedSpanCreator(t *testing.T) {
	// Create test tracer
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "span-creator-test"

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

	creator := NewOptimizedSpanCreator(manager.GetTracer())

	// Test basic span creation
	t.Run("BasicSpanCreation", func(t *testing.T) {
		_, span := creator.CreateSpan(ctx, "test-span",
			attribute.String("key1", "value1"),
			attribute.Int("key2", 42))

		if span == nil {
			t.Fatal("Expected non-nil span")
		}

		span.End()
	})

	// Test span creation with builder
	t.Run("SpanCreationWithBuilder", func(t *testing.T) {
		_, span := creator.CreateSpanWithBuilder(ctx, "test-span-builder",
			func(attrs []attribute.KeyValue) []attribute.KeyValue {
				attrs = append(attrs, attribute.String("dynamic", "value"))
				attrs = append(attrs, attribute.Int("count", 10))
				return attrs
			})

		if span == nil {
			t.Fatal("Expected non-nil span")
		}

		span.End()
	})
}

// TestOptimizedMetricRecorder tests the optimized metric recorder
func TestOptimizedMetricRecorder(t *testing.T) {
	// Create test meter
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "metric-recorder-test"

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

	recorder := NewOptimizedMetricRecorder(manager.GetMeter())

	// Test counter creation and reuse
	t.Run("CounterCreationAndReuse", func(t *testing.T) {
		counter1, err := recorder.GetOrCreateCounter("test_counter", "Test counter")
		if err != nil {
			t.Fatalf("Failed to create counter: %v", err)
		}

		counter2, err := recorder.GetOrCreateCounter("test_counter", "Test counter")
		if err != nil {
			t.Fatalf("Failed to get existing counter: %v", err)
		}

		// Should be the same instance
		if counter1 != counter2 {
			t.Error("Expected same counter instance for same name")
		}
	})

	// Test histogram creation and reuse
	t.Run("HistogramCreationAndReuse", func(t *testing.T) {
		histogram1, err := recorder.GetOrCreateHistogram("test_histogram", "Test histogram", "ms")
		if err != nil {
			t.Fatalf("Failed to create histogram: %v", err)
		}

		histogram2, err := recorder.GetOrCreateHistogram("test_histogram", "Test histogram", "ms")
		if err != nil {
			t.Fatalf("Failed to get existing histogram: %v", err)
		}

		// Should be the same instance
		if histogram1 != histogram2 {
			t.Error("Expected same histogram instance for same name")
		}
	})

	// Test recording with builder
	t.Run("RecordingWithBuilder", func(t *testing.T) {
		err := recorder.RecordCounterWithBuilder(ctx, "test_counter_builder", 1,
			func(attrs []attribute.KeyValue) []attribute.KeyValue {
				attrs = append(attrs, attribute.String("method", "POST"))
				attrs = append(attrs, attribute.Int("status", 200))
				return attrs
			})

		if err != nil {
			t.Errorf("Failed to record counter with builder: %v", err)
		}

		err = recorder.RecordHistogramWithBuilder(ctx, "test_histogram_builder", 123.45, "ms",
			func(attrs []attribute.KeyValue) []attribute.KeyValue {
				attrs = append(attrs, attribute.String("endpoint", "/api/test"))
				attrs = append(attrs, attribute.String("method", "GET"))
				return attrs
			})

		if err != nil {
			t.Errorf("Failed to record histogram with builder: %v", err)
		}
	})
}

// TestPerformanceMonitor tests the performance monitor
func TestPerformanceMonitor(t *testing.T) {
	monitor := NewPerformanceMonitor(true)

	// Test operation measurement
	t.Run("OperationMeasurement", func(t *testing.T) {
		monitor.MeasureOperation("test_operation", func() {
			time.Sleep(10 * time.Millisecond)
		})

		stats := monitor.GetStats()
		if len(stats) != 1 {
			t.Errorf("Expected 1 operation stat, got %d", len(stats))
		}

		opStats, exists := stats["test_operation"]
		if !exists {
			t.Fatal("Expected test_operation stats")
		}

		if opStats.Count != 1 {
			t.Errorf("Expected count 1, got %d", opStats.Count)
		}

		if opStats.TotalTime < 10*time.Millisecond {
			t.Errorf("Expected total time >= 10ms, got %v", opStats.TotalTime)
		}
	})

	// Test multiple operations
	t.Run("MultipleOperations", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			monitor.MeasureOperation("multi_test", func() {
				time.Sleep(time.Millisecond)
			})
		}

		stats := monitor.GetStats()
		opStats := stats["multi_test"]

		if opStats.Count != 5 {
			t.Errorf("Expected count 5, got %d", opStats.Count)
		}

		if opStats.MinTime <= 0 {
			t.Errorf("Expected positive min time, got %v", opStats.MinTime)
		}

		if opStats.MaxTime <= 0 {
			t.Errorf("Expected positive max time, got %v", opStats.MaxTime)
		}
	})

	// Test disabled monitor
	t.Run("DisabledMonitor", func(t *testing.T) {
		disabledMonitor := NewPerformanceMonitor(false)

		disabledMonitor.MeasureOperation("disabled_test", func() {
			time.Sleep(time.Millisecond)
		})

		stats := disabledMonitor.GetStats()
		if len(stats) != 0 {
			t.Errorf("Expected no stats for disabled monitor, got %d", len(stats))
		}
	})

	// Test reset
	t.Run("Reset", func(t *testing.T) {
		monitor.Reset()
		stats := monitor.GetStats()
		if len(stats) != 0 {
			t.Errorf("Expected no stats after reset, got %d", len(stats))
		}
	})
}

// TestResourceManager tests the resource manager
func TestResourceManager(t *testing.T) {
	rm := NewResourceManager()
	defer rm.Stop()

	// Test manager registration
	t.Run("ManagerRegistration", func(t *testing.T) {
		config := DefaultOTELConfig()
		config.Enabled = true
		config.ServiceName = "resource-test-1"

		manager1, err := NewOTELManager(config)
		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}

		config.ServiceName = "resource-test-2"
		manager2, err := NewOTELManager(config)
		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}

		rm.RegisterManager(manager1)
		rm.RegisterManager(manager2)

		usage := rm.GetResourceUsage()
		if usage["managers_count"].(int) != 2 {
			t.Errorf("Expected 2 managers, got %d", usage["managers_count"].(int))
		}

		rm.UnregisterManager(manager1)
		usage = rm.GetResourceUsage()
		if usage["managers_count"].(int) != 1 {
			t.Errorf("Expected 1 manager after unregister, got %d", usage["managers_count"].(int))
		}

		rm.UnregisterManager(manager2)
	})

	// Test resource usage reporting
	t.Run("ResourceUsage", func(t *testing.T) {
		usage := rm.GetResourceUsage()

		expectedKeys := []string{"goroutines", "memory_alloc", "memory_sys", "gc_cycles", "managers_count"}
		for _, key := range expectedKeys {
			if _, exists := usage[key]; !exists {
				t.Errorf("Expected key %s in resource usage", key)
			}
		}
	})
}

// TestGlobalResourceManagerCleanup tests that the global resource manager can be stopped properly
func TestGlobalResourceManagerCleanup(t *testing.T) {
	// Get initial goroutine count
	initialGoroutines := runtime.NumGoroutine()

	// Register a test manager with global resource manager
	config := DefaultOTELConfig()
	config.Enabled = false // Keep disabled to avoid complex cleanup
	config.ServiceName = "global-test"

	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	RegisterGlobalManager(manager)

	// Get resource usage to verify the global manager is working
	usage := GetGlobalResourceUsage()
	if usage == nil {
		t.Fatal("Expected resource usage to be available")
	}

	// Stop the global resource manager
	StopGlobalResourceManager()

	// Give some time for the goroutine to clean up
	time.Sleep(50 * time.Millisecond)

	// Verify no significant goroutine leak (allowing some tolerance for test framework goroutines)
	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines+2 {
		t.Errorf("Potential goroutine leak: started with %d, ended with %d goroutines",
			initialGoroutines, finalGoroutines)
	}

	// Cleanup the manager
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Errorf("Failed to shutdown manager: %v", err)
	}
}

// TestOptimizedAttributeBuilder tests the optimized attribute builder
func TestOptimizedAttributeBuilder(t *testing.T) {
	// Test basic building
	t.Run("BasicBuilding", func(t *testing.T) {
		builder := NewOptimizedAttributeBuilder()
		attrs := builder.
			String("string_key", "string_value").
			Int("int_key", 42).
			Int64("int64_key", 123456789).
			Float64("float64_key", 3.14159).
			Bool("bool_key", true).
			StringSlice("slice_key", []string{"a", "b", "c"}).
			Build()

		if len(attrs) != 6 {
			t.Errorf("Expected 6 attributes, got %d", len(attrs))
		}

		// Verify attribute values
		expectedAttrs := map[string]interface{}{
			"string_key":  "string_value",
			"int_key":     42,
			"int64_key":   int64(123456789),
			"float64_key": 3.14159,
			"bool_key":    true,
		}

		for _, attr := range attrs {
			key := string(attr.Key)
			if key == "slice_key" {
				slice := attr.Value.AsStringSlice()
				if len(slice) != 3 || slice[0] != "a" || slice[1] != "b" || slice[2] != "c" {
					t.Errorf("Expected slice [a, b, c], got %v", slice)
				}
				continue
			}

			expectedValue, exists := expectedAttrs[key]
			if !exists {
				t.Errorf("Unexpected attribute key: %s", key)
				continue
			}

			var actualValue interface{}
			switch expectedValue.(type) {
			case string:
				actualValue = attr.Value.AsString()
			case int:
				actualValue = int(attr.Value.AsInt64())
			case int64:
				actualValue = attr.Value.AsInt64()
			case float64:
				actualValue = attr.Value.AsFloat64()
			case bool:
				actualValue = attr.Value.AsBool()
			}

			if actualValue != expectedValue {
				t.Errorf("Expected %s=%v, got %v", key, expectedValue, actualValue)
			}
		}
	})

	// Test build and apply
	t.Run("BuildAndApply", func(t *testing.T) {
		builder := NewOptimizedAttributeBuilder()
		var receivedAttrs []attribute.KeyValue

		builder.
			String("test_key", "test_value").
			Int("count", 10).
			BuildAndApply(func(attrs []attribute.KeyValue) {
				receivedAttrs = make([]attribute.KeyValue, len(attrs))
				copy(receivedAttrs, attrs)
			})

		if len(receivedAttrs) != 2 {
			t.Errorf("Expected 2 attributes in apply function, got %d", len(receivedAttrs))
		}
	})
}

// BenchmarkAttributePool benchmarks the attribute pool performance
func BenchmarkAttributePool(b *testing.B) {
	pool := NewAttributePool()

	b.Run("WithPool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			attrs := pool.Get()
			attrs = append(attrs, attribute.String("key1", "value1"))
			attrs = append(attrs, attribute.String("key2", "value2"))
			attrs = append(attrs, attribute.Int("key3", i))
			pool.Put(attrs)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			attrs := make([]attribute.KeyValue, 0, 8)
			attrs = append(attrs, attribute.String("key1", "value1"))
			attrs = append(attrs, attribute.String("key2", "value2"))
			attrs = append(attrs, attribute.Int("key3", i))
			_ = attrs
		}
	})
}

// BenchmarkOptimizedSpanCreation benchmarks optimized span creation
func BenchmarkOptimizedSpanCreation(b *testing.B) {
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "benchmark-test"

	manager, err := NewOTELManager(config)
	if err != nil {
		b.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		b.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	creator := NewOptimizedSpanCreator(manager.GetTracer())
	tracer := manager.GetTracer()

	b.Run("OptimizedCreation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, span := creator.CreateSpan(ctx, "benchmark-span",
				attribute.String("operation", "test"),
				attribute.Int("iteration", i))
			span.End()
		}
	})

	b.Run("StandardCreation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, span := tracer.Start(ctx, "benchmark-span",
				trace.WithAttributes(
					attribute.String("operation", "test"),
					attribute.Int("iteration", i)))
			span.End()
		}
	})

	b.Run("OptimizedWithBuilder", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, span := creator.CreateSpanWithBuilder(ctx, "benchmark-span",
				func(attrs []attribute.KeyValue) []attribute.KeyValue {
					attrs = append(attrs, attribute.String("operation", "test"))
					attrs = append(attrs, attribute.Int("iteration", i))
					return attrs
				})
			span.End()
		}
	})
}

// BenchmarkOptimizedMetricRecording benchmarks optimized metric recording
func BenchmarkOptimizedMetricRecording(b *testing.B) {
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "benchmark-test"

	manager, err := NewOTELManager(config)
	if err != nil {
		b.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		b.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	recorder := NewOptimizedMetricRecorder(manager.GetMeter())
	meter := manager.GetMeter()

	// Pre-create standard counter for comparison
	standardCounter, _ := meter.Int64Counter("standard_counter")

	b.Run("OptimizedCounter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			recorder.RecordCounterWithBuilder(ctx, "optimized_counter", 1,
				func(attrs []attribute.KeyValue) []attribute.KeyValue {
					attrs = append(attrs, attribute.String("method", "GET"))
					attrs = append(attrs, attribute.Int("status", 200))
					return attrs
				})
		}
	})

	b.Run("StandardCounter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			standardCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("method", "GET"),
				attribute.Int("status", 200)))
		}
	})
}

// TestConcurrentOptimizations tests optimizations under concurrent load
func TestConcurrentOptimizations(t *testing.T) {
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

	creator := NewOptimizedSpanCreator(manager.GetTracer())
	recorder := NewOptimizedMetricRecorder(manager.GetMeter())

	const numWorkers = 10
	const operationsPerWorker = 100

	var wg sync.WaitGroup

	// Test concurrent span creation
	t.Run("ConcurrentSpanCreation", func(t *testing.T) {
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < operationsPerWorker; j++ {
					_, span := creator.CreateSpanWithBuilder(ctx, "concurrent-span",
						func(attrs []attribute.KeyValue) []attribute.KeyValue {
							attrs = append(attrs, attribute.Int("worker_id", workerID))
							attrs = append(attrs, attribute.Int("operation_id", j))
							return attrs
						})
					span.End()
				}
			}(i)
		}
		wg.Wait()
	})

	// Test concurrent metric recording
	t.Run("ConcurrentMetricRecording", func(t *testing.T) {
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < operationsPerWorker; j++ {
					recorder.RecordCounterWithBuilder(ctx, "concurrent_counter", 1,
						func(attrs []attribute.KeyValue) []attribute.KeyValue {
							attrs = append(attrs, attribute.Int("worker_id", workerID))
							return attrs
						})

					recorder.RecordHistogramWithBuilder(ctx, "concurrent_histogram", float64(j), "ms",
						func(attrs []attribute.KeyValue) []attribute.KeyValue {
							attrs = append(attrs, attribute.Int("worker_id", workerID))
							return attrs
						})
				}
			}(i)
		}
		wg.Wait()
	})
}

// TestMemoryOptimizations tests memory usage optimizations
func TestMemoryOptimizations(t *testing.T) {
	// Measure memory usage with optimizations
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "memory-test"

	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}

	creator := NewOptimizedSpanCreator(manager.GetTracer())
	recorder := NewOptimizedMetricRecorder(manager.GetMeter())

	// Perform many operations
	const numOperations = 1000
	for i := 0; i < numOperations; i++ {
		// Create spans with optimized builder
		_, span := creator.CreateSpanWithBuilder(ctx, "memory-test-span",
			func(attrs []attribute.KeyValue) []attribute.KeyValue {
				attrs = append(attrs, attribute.String("operation", "memory-test"))
				attrs = append(attrs, attribute.Int("iteration", i))
				attrs = append(attrs, attribute.Bool("optimized", true))
				return attrs
			})
		span.End()

		// Record metrics with optimized builder
		recorder.RecordCounterWithBuilder(ctx, "memory_test_counter", 1,
			func(attrs []attribute.KeyValue) []attribute.KeyValue {
				attrs = append(attrs, attribute.String("test_type", "memory"))
				attrs = append(attrs, attribute.Int("iteration", i))
				return attrs
			})
	}

	manager.Shutdown(ctx)

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Calculate memory increase, handling potential underflow
	var memoryIncrease uint64
	if memAfter.Alloc > memBefore.Alloc {
		memoryIncrease = memAfter.Alloc - memBefore.Alloc
	} else {
		// Memory usage decreased (GC ran), which is good
		memoryIncrease = 0
	}

	t.Logf("Memory usage for %d operations: %d bytes", numOperations, memoryIncrease)

	// Memory increase should be reasonable (less than 10MB for 1000 operations)
	const maxMemoryIncrease = 10 * 1024 * 1024 // 10MB
	if memoryIncrease > maxMemoryIncrease {
		t.Errorf("Memory usage too high: %d bytes (max allowed: %d)", memoryIncrease, maxMemoryIncrease)
	}
}
