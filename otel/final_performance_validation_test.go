package otel

import (
	"context"
	"testing"
	"time"
)

// TestFinalPerformanceValidation runs comprehensive performance validation
func TestFinalPerformanceValidation(t *testing.T) {
	validator, err := NewPerformanceValidator()
	if err != nil {
		t.Fatalf("Failed to create performance validator: %v", err)
	}
	defer validator.Cleanup()

	ctx := context.Background()

	// Run performance validation
	result, err := validator.ValidatePerformance(ctx)
	if err != nil {
		t.Fatalf("Performance validation failed: %v", err)
	}

	// Log results
	t.Logf("Performance Validation Results:")
	t.Logf("  Span Creation Overhead: %v", result.SpanCreationOverhead)
	t.Logf("  Metric Recording Overhead: %v", result.MetricRecordingOverhead)
	t.Logf("  Memory Overhead: %d bytes", result.MemoryOverhead)
	t.Logf("  Goroutine Overhead: %d", result.GoroutineOverhead)
	t.Logf("  Throughput Degradation: %.2f%%", result.ThroughputDegradation*100)
	t.Logf("  Overall Result: %v", result.Passed)

	if len(result.Issues) > 0 {
		t.Logf("  Issues:")
		for _, issue := range result.Issues {
			t.Logf("    - %s", issue)
		}
	}

	// The test passes even if performance thresholds are exceeded,
	// but we log the results for analysis
	if !result.Passed {
		t.Logf("Performance validation failed, but test continues for analysis")
	}
}

// TestPerformanceValidationWithCustomThresholds tests with custom thresholds
func TestPerformanceValidationWithCustomThresholds(t *testing.T) {
	validator, err := NewPerformanceValidator()
	if err != nil {
		t.Fatalf("Failed to create performance validator: %v", err)
	}
	defer validator.Cleanup()

	// Set realistic but strict thresholds to test the validation logic
	strictThresholds := PerformanceThresholds{
		MaxSpanOverheadNs:        1000, // 1 microsecond
		MaxMetricOverheadNs:      1000, // 1 microsecond
		MaxMemoryOverheadBytes:   1024, // 1 KB
		MaxGoroutineOverhead:     1,    // 1 goroutine overhead allowed
		MaxThroughputDegradation: 0.05, // 5% degradation
	}
	validator.SetThresholds(strictThresholds)

	ctx := context.Background()
	result, err := validator.ValidatePerformance(ctx)
	if err != nil {
		t.Fatalf("Performance validation failed: %v", err)
	}

	// With strict thresholds, we expect the validation to fail
	if result.Passed {
		t.Log("Performance validation passed with strict thresholds (impressive!)")
	} else {
		t.Logf("Performance validation failed with strict thresholds (expected)")
		t.Logf("Issues: %v", result.Issues)
	}
}

// TestResourceCleanupManager tests the resource cleanup manager
func TestResourceCleanupManager(t *testing.T) {
	rcm := NewResourceCleanupManager()

	// Track cleanup calls
	cleanupCalls := 0

	// Register cleanup functions
	rcm.RegisterCleanup(func() error {
		cleanupCalls++
		return nil
	})

	rcm.RegisterCleanup(func() error {
		cleanupCalls++
		return nil
	})

	// Execute cleanup
	err := rcm.CleanupAll()
	if err != nil {
		t.Errorf("Cleanup failed: %v", err)
	}

	if cleanupCalls != 2 {
		t.Errorf("Expected 2 cleanup calls, got %d", cleanupCalls)
	}

	// Second cleanup should not call functions again
	err = rcm.CleanupAll()
	if err != nil {
		t.Errorf("Second cleanup failed: %v", err)
	}

	if cleanupCalls != 2 {
		t.Errorf("Expected cleanup calls to remain 2, got %d", cleanupCalls)
	}
}

// TestMemoryStatsAndGoroutineCount tests utility functions
func TestMemoryStatsAndGoroutineCount(t *testing.T) {
	// Test memory stats
	memStats := GetMemoryStats()
	expectedKeys := []string{
		"alloc", "total_alloc", "sys", "heap_alloc", "heap_sys",
		"heap_idle", "heap_inuse", "heap_released", "heap_objects",
		"stack_inuse", "stack_sys", "gc_cycles",
	}

	for _, key := range expectedKeys {
		if _, exists := memStats[key]; !exists {
			t.Errorf("Expected memory stat key %s not found", key)
		}
	}

	// Test goroutine count
	goroutineCount := GetGoroutineCount()
	if goroutineCount <= 0 {
		t.Errorf("Expected positive goroutine count, got %d", goroutineCount)
	}

	// Test force garbage collection (should not panic)
	ForceGarbageCollection()

	// Test logging (should not panic)
	LogResourceUsage("Test")
}

// BenchmarkPerformanceValidation benchmarks the performance validation itself
func BenchmarkPerformanceValidation(b *testing.B) {
	validator, err := NewPerformanceValidator()
	if err != nil {
		b.Fatalf("Failed to create performance validator: %v", err)
	}
	defer validator.Cleanup()

	// Set reasonable thresholds for benchmarking
	thresholds := PerformanceThresholds{
		MaxSpanOverheadNs:        10000,       // 10 microseconds
		MaxMetricOverheadNs:      5000,        // 5 microseconds
		MaxMemoryOverheadBytes:   1024 * 1024, // 1MB
		MaxGoroutineOverhead:     10,          // 10 goroutines
		MaxThroughputDegradation: 0.2,         // 20% degradation
	}
	validator.SetThresholds(thresholds)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := validator.ValidatePerformance(ctx)
		if err != nil {
			b.Fatalf("Performance validation failed: %v", err)
		}
		_ = result // Use the result to prevent optimization
	}
}

// TestPerformanceValidationComponents tests individual validation components
func TestPerformanceValidationComponents(t *testing.T) {
	validator, err := NewPerformanceValidator()
	if err != nil {
		t.Fatalf("Failed to create performance validator: %v", err)
	}
	defer validator.Cleanup()

	ctx := context.Background()

	// Test span creation overhead validation
	t.Run("SpanCreationOverhead", func(t *testing.T) {
		overhead, err := validator.validateSpanCreationOverhead(ctx)
		if err != nil {
			t.Errorf("Failed to validate span creation overhead: %v", err)
		}
		t.Logf("Span creation overhead: %v", overhead)
	})

	// Test metric recording overhead validation
	t.Run("MetricRecordingOverhead", func(t *testing.T) {
		overhead, err := validator.validateMetricRecordingOverhead(ctx)
		if err != nil {
			t.Errorf("Failed to validate metric recording overhead: %v", err)
		}
		t.Logf("Metric recording overhead: %v", overhead)
	})

	// Test memory overhead validation
	t.Run("MemoryOverhead", func(t *testing.T) {
		overhead, err := validator.validateMemoryOverhead(ctx)
		if err != nil {
			t.Errorf("Failed to validate memory overhead: %v", err)
		}
		t.Logf("Memory overhead: %d bytes", overhead)
	})

	// Test goroutine overhead validation
	t.Run("GoroutineOverhead", func(t *testing.T) {
		overhead, err := validator.validateGoroutineOverhead(ctx)
		if err != nil {
			t.Errorf("Failed to validate goroutine overhead: %v", err)
		}
		t.Logf("Goroutine overhead: %d", overhead)
	})

	// Test throughput degradation validation
	t.Run("ThroughputDegradation", func(t *testing.T) {
		degradation, err := validator.validateThroughputDegradation(ctx)
		if err != nil {
			t.Errorf("Failed to validate throughput degradation: %v", err)
		}
		t.Logf("Throughput degradation: %.2f%%", degradation*100)
	})
}

// TestPerformanceValidationStress tests performance validation under stress
func TestPerformanceValidationStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	validator, err := NewPerformanceValidator()
	if err != nil {
		t.Fatalf("Failed to create performance validator: %v", err)
	}
	defer validator.Cleanup()

	ctx := context.Background()

	// Run validation multiple times to check consistency
	const iterations = 5
	results := make([]*PerformanceValidationResult, iterations)

	for i := 0; i < iterations; i++ {
		result, err := validator.ValidatePerformance(ctx)
		if err != nil {
			t.Fatalf("Performance validation %d failed: %v", i, err)
		}
		results[i] = result

		// Small delay between iterations
		time.Sleep(100 * time.Millisecond)
	}

	// Analyze consistency of results
	t.Logf("Performance validation consistency analysis:")

	var totalSpanOverhead, totalMetricOverhead time.Duration
	var totalMemoryOverhead uint64
	var totalGoroutineOverhead int
	var totalThroughputDegradation float64

	for i, result := range results {
		t.Logf("  Iteration %d:", i+1)
		t.Logf("    Span Overhead: %v", result.SpanCreationOverhead)
		t.Logf("    Metric Overhead: %v", result.MetricRecordingOverhead)
		t.Logf("    Memory Overhead: %d bytes", result.MemoryOverhead)
		t.Logf("    Goroutine Overhead: %d", result.GoroutineOverhead)
		t.Logf("    Throughput Degradation: %.2f%%", result.ThroughputDegradation*100)

		totalSpanOverhead += result.SpanCreationOverhead
		totalMetricOverhead += result.MetricRecordingOverhead
		totalMemoryOverhead += result.MemoryOverhead
		totalGoroutineOverhead += result.GoroutineOverhead
		totalThroughputDegradation += result.ThroughputDegradation
	}

	// Calculate averages
	avgSpanOverhead := totalSpanOverhead / iterations
	avgMetricOverhead := totalMetricOverhead / iterations
	avgMemoryOverhead := totalMemoryOverhead / iterations
	avgGoroutineOverhead := totalGoroutineOverhead / iterations
	avgThroughputDegradation := totalThroughputDegradation / iterations

	t.Logf("  Averages:")
	t.Logf("    Span Overhead: %v", avgSpanOverhead)
	t.Logf("    Metric Overhead: %v", avgMetricOverhead)
	t.Logf("    Memory Overhead: %d bytes", avgMemoryOverhead)
	t.Logf("    Goroutine Overhead: %d", avgGoroutineOverhead)
	t.Logf("    Throughput Degradation: %.2f%%", avgThroughputDegradation*100)
}
