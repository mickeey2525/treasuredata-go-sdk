package otel

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// PerformanceValidationResult holds the results of performance validation
type PerformanceValidationResult struct {
	SpanCreationOverhead    time.Duration
	MetricRecordingOverhead time.Duration
	MemoryOverhead          uint64
	GoroutineOverhead       int
	ThroughputDegradation   float64
	Passed                  bool
	Issues                  []string
}

// PerformanceValidator validates the performance characteristics of OTEL integration
type PerformanceValidator struct {
	enabledManager  *OTELManager
	disabledManager *OTELManager
	thresholds      PerformanceThresholds
}

// PerformanceThresholds defines acceptable performance thresholds
type PerformanceThresholds struct {
	MaxSpanOverheadNs        int64   // Maximum acceptable span creation overhead in nanoseconds
	MaxMetricOverheadNs      int64   // Maximum acceptable metric recording overhead in nanoseconds
	MaxMemoryOverheadBytes   uint64  // Maximum acceptable memory overhead in bytes
	MaxGoroutineOverhead     int     // Maximum acceptable goroutine overhead
	MaxThroughputDegradation float64 // Maximum acceptable throughput degradation (0.0-1.0)
}

// DefaultPerformanceThresholds returns default performance thresholds
func DefaultPerformanceThresholds() PerformanceThresholds {
	return PerformanceThresholds{
		MaxSpanOverheadNs:        1000,        // 1 microsecond
		MaxMetricOverheadNs:      500,         // 0.5 microseconds
		MaxMemoryOverheadBytes:   1024 * 1024, // 1MB
		MaxGoroutineOverhead:     5,           // 5 goroutines
		MaxThroughputDegradation: 0.1,         // 10% degradation
	}
}

// NewPerformanceValidator creates a new performance validator
func NewPerformanceValidator() (*PerformanceValidator, error) {
	// Create enabled OTEL manager
	enabledConfig := DefaultOTELConfig()
	enabledConfig.Enabled = true
	enabledConfig.ServiceName = "performance-validation"
	enabledConfig.BatchTimeout = 100 * time.Millisecond
	enabledConfig.BatchSize = 100

	enabledManager, err := NewOTELManager(enabledConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create enabled OTEL manager: %w", err)
	}

	err = enabledManager.Initialize(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize enabled OTEL manager: %w", err)
	}

	// Create disabled OTEL manager
	disabledConfig := DefaultOTELConfig()
	disabledConfig.Enabled = false
	disabledConfig.ServiceName = "performance-validation-disabled"

	disabledManager, err := NewOTELManager(disabledConfig)
	if err != nil {
		enabledManager.Shutdown(context.Background())
		return nil, fmt.Errorf("failed to create disabled OTEL manager: %w", err)
	}

	err = disabledManager.Initialize(context.Background())
	if err != nil {
		enabledManager.Shutdown(context.Background())
		return nil, fmt.Errorf("failed to initialize disabled OTEL manager: %w", err)
	}

	return &PerformanceValidator{
		enabledManager:  enabledManager,
		disabledManager: disabledManager,
		thresholds:      DefaultPerformanceThresholds(),
	}, nil
}

// SetThresholds sets custom performance thresholds
func (pv *PerformanceValidator) SetThresholds(thresholds PerformanceThresholds) {
	pv.thresholds = thresholds
}

// ValidatePerformance performs comprehensive performance validation
func (pv *PerformanceValidator) ValidatePerformance(ctx context.Context) (*PerformanceValidationResult, error) {
	result := &PerformanceValidationResult{
		Passed: true,
		Issues: make([]string, 0),
	}

	// Validate span creation overhead
	spanOverhead, err := pv.validateSpanCreationOverhead(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate span creation overhead: %w", err)
	}
	result.SpanCreationOverhead = spanOverhead

	if spanOverhead.Nanoseconds() > pv.thresholds.MaxSpanOverheadNs {
		result.Passed = false
		result.Issues = append(result.Issues, fmt.Sprintf("Span creation overhead too high: %v (max: %dns)",
			spanOverhead, pv.thresholds.MaxSpanOverheadNs))
	}

	// Validate metric recording overhead
	metricOverhead, err := pv.validateMetricRecordingOverhead(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate metric recording overhead: %w", err)
	}
	result.MetricRecordingOverhead = metricOverhead

	if metricOverhead.Nanoseconds() > pv.thresholds.MaxMetricOverheadNs {
		result.Passed = false
		result.Issues = append(result.Issues, fmt.Sprintf("Metric recording overhead too high: %v (max: %dns)",
			metricOverhead, pv.thresholds.MaxMetricOverheadNs))
	}

	// Validate memory overhead
	memoryOverhead, err := pv.validateMemoryOverhead(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate memory overhead: %w", err)
	}
	result.MemoryOverhead = memoryOverhead

	if memoryOverhead > pv.thresholds.MaxMemoryOverheadBytes {
		result.Passed = false
		result.Issues = append(result.Issues, fmt.Sprintf("Memory overhead too high: %d bytes (max: %d bytes)",
			memoryOverhead, pv.thresholds.MaxMemoryOverheadBytes))
	}

	// Validate goroutine overhead
	goroutineOverhead, err := pv.validateGoroutineOverhead(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate goroutine overhead: %w", err)
	}
	result.GoroutineOverhead = goroutineOverhead

	if goroutineOverhead > pv.thresholds.MaxGoroutineOverhead {
		result.Passed = false
		result.Issues = append(result.Issues, fmt.Sprintf("Goroutine overhead too high: %d (max: %d)",
			goroutineOverhead, pv.thresholds.MaxGoroutineOverhead))
	}

	// Validate throughput degradation
	throughputDegradation, err := pv.validateThroughputDegradation(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate throughput degradation: %w", err)
	}
	result.ThroughputDegradation = throughputDegradation

	if throughputDegradation > pv.thresholds.MaxThroughputDegradation {
		result.Passed = false
		result.Issues = append(result.Issues, fmt.Sprintf("Throughput degradation too high: %.2f%% (max: %.2f%%)",
			throughputDegradation*100, pv.thresholds.MaxThroughputDegradation*100))
	}

	return result, nil
}

// validateSpanCreationOverhead measures the overhead of span creation
func (pv *PerformanceValidator) validateSpanCreationOverhead(ctx context.Context) (time.Duration, error) {
	const iterations = 10000

	// Measure with OTEL enabled
	enabledTracer := pv.enabledManager.GetTracer()
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_, span := enabledTracer.Start(ctx, "test-span")
		span.SetAttributes(attribute.String("test", "value"))
		span.End()
	}
	enabledDuration := time.Since(start)

	// Measure with OTEL disabled
	disabledTracer := pv.disabledManager.GetTracer()
	start = time.Now()
	for i := 0; i < iterations; i++ {
		_, span := disabledTracer.Start(ctx, "test-span")
		span.SetAttributes(attribute.String("test", "value"))
		span.End()
	}
	disabledDuration := time.Since(start)

	// Calculate per-operation overhead
	overhead := (enabledDuration - disabledDuration) / iterations
	return overhead, nil
}

// validateMetricRecordingOverhead measures the overhead of metric recording
func (pv *PerformanceValidator) validateMetricRecordingOverhead(ctx context.Context) (time.Duration, error) {
	const iterations = 10000

	// Measure with OTEL enabled
	enabledMeter := pv.enabledManager.GetMeter()
	enabledCounter, _ := enabledMeter.Int64Counter("test_counter")
	start := time.Now()
	for i := 0; i < iterations; i++ {
		enabledCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("test", "value")))
	}
	enabledDuration := time.Since(start)

	// Measure with OTEL disabled
	disabledMeter := pv.disabledManager.GetMeter()
	disabledCounter, _ := disabledMeter.Int64Counter("test_counter")
	start = time.Now()
	for i := 0; i < iterations; i++ {
		disabledCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("test", "value")))
	}
	disabledDuration := time.Since(start)

	// Calculate per-operation overhead
	overhead := (enabledDuration - disabledDuration) / iterations
	return overhead, nil
}

// validateMemoryOverhead measures the memory overhead of OTEL operations
func (pv *PerformanceValidator) validateMemoryOverhead(ctx context.Context) (uint64, error) {
	const iterations = 1000

	// Force garbage collection
	runtime.GC()
	runtime.GC()

	// Measure baseline memory
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Perform operations with OTEL enabled
	enabledTracer := pv.enabledManager.GetTracer()
	enabledMeter := pv.enabledManager.GetMeter()
	enabledCounter, _ := enabledMeter.Int64Counter("memory_test_counter")

	for i := 0; i < iterations; i++ {
		_, span := enabledTracer.Start(ctx, "memory-test-span")
		span.SetAttributes(
			attribute.String("operation", "memory-test"),
			attribute.Int("iteration", i),
		)
		span.End()

		enabledCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("test", "memory"),
			attribute.Int("iteration", i),
		))
	}

	// Force garbage collection and measure memory
	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Calculate memory overhead
	var memoryOverhead uint64
	if memAfter.Alloc > memBefore.Alloc {
		memoryOverhead = memAfter.Alloc - memBefore.Alloc
	}

	return memoryOverhead, nil
}

// validateGoroutineOverhead measures the goroutine overhead
func (pv *PerformanceValidator) validateGoroutineOverhead(ctx context.Context) (int, error) {
	// Measure baseline goroutines
	baselineGoroutines := runtime.NumGoroutine()

	// Create additional OTEL manager to see goroutine impact
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "goroutine-test"

	manager, err := NewOTELManager(config)
	if err != nil {
		return 0, err
	}

	err = manager.Initialize(ctx)
	if err != nil {
		return 0, err
	}

	// Measure goroutines after initialization
	afterInitGoroutines := runtime.NumGoroutine()

	// Perform some operations
	tracer := manager.GetTracer()
	meter := manager.GetMeter()
	counter, _ := meter.Int64Counter("goroutine_test_counter")

	for i := 0; i < 100; i++ {
		_, span := tracer.Start(ctx, "goroutine-test-span")
		span.End()
		counter.Add(ctx, 1)
	}

	// Measure goroutines after operations
	afterOpsGoroutines := runtime.NumGoroutine()

	// Shutdown and measure final goroutines
	manager.Shutdown(ctx)
	time.Sleep(100 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()

	// Calculate maximum overhead during the test
	maxOverhead := 0
	if afterInitGoroutines-baselineGoroutines > maxOverhead {
		maxOverhead = afterInitGoroutines - baselineGoroutines
	}
	if afterOpsGoroutines-baselineGoroutines > maxOverhead {
		maxOverhead = afterOpsGoroutines - baselineGoroutines
	}
	if finalGoroutines-baselineGoroutines > maxOverhead {
		maxOverhead = finalGoroutines - baselineGoroutines
	}

	return maxOverhead, nil
}

// validateThroughputDegradation measures the throughput impact of OTEL
func (pv *PerformanceValidator) validateThroughputDegradation(ctx context.Context) (float64, error) {
	const duration = 1 * time.Second
	const numWorkers = 4

	// Measure throughput with OTEL enabled
	enabledThroughput := pv.measureThroughput(ctx, pv.enabledManager, duration, numWorkers)

	// Measure throughput with OTEL disabled
	disabledThroughput := pv.measureThroughput(ctx, pv.disabledManager, duration, numWorkers)

	// Calculate degradation
	if disabledThroughput == 0 {
		return 0, nil
	}

	degradation := float64(disabledThroughput-enabledThroughput) / float64(disabledThroughput)
	if degradation < 0 {
		degradation = 0 // OTEL actually improved performance
	}

	return degradation, nil
}

// measureThroughput measures operations per second
func (pv *PerformanceValidator) measureThroughput(ctx context.Context, manager *OTELManager, duration time.Duration, numWorkers int) int64 {
	tracer := manager.GetTracer()
	meter := manager.GetMeter()
	counter, _ := meter.Int64Counter("throughput_test_counter")

	var totalOps int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ops := int64(0)
			for {
				select {
				case <-stopCh:
					totalOps += ops
					return
				default:
					// Perform a typical operation
					_, span := tracer.Start(ctx, "throughput-test")
					span.SetAttributes(
						attribute.Int("worker_id", workerID),
						attribute.Int64("operation", ops),
					)
					counter.Add(ctx, 1, metric.WithAttributes(
						attribute.Int("worker_id", workerID),
					))
					span.End()
					ops++
				}
			}
		}(i)
	}

	// Run for specified duration
	time.Sleep(duration)
	close(stopCh)
	wg.Wait()

	return totalOps
}

// Cleanup cleans up the validator resources
func (pv *PerformanceValidator) Cleanup() error {
	ctx := context.Background()
	var lastError error

	if pv.enabledManager != nil {
		if err := pv.enabledManager.Shutdown(ctx); err != nil {
			lastError = err
		}
	}

	if pv.disabledManager != nil {
		if err := pv.disabledManager.Shutdown(ctx); err != nil {
			lastError = err
		}
	}

	return lastError
}

// ResourceCleanupManager provides utilities for cleaning up OTEL resources
type ResourceCleanupManager struct {
	managers []func() error
	mutex    sync.Mutex
}

// NewResourceCleanupManager creates a new resource cleanup manager
func NewResourceCleanupManager() *ResourceCleanupManager {
	return &ResourceCleanupManager{
		managers: make([]func() error, 0),
	}
}

// RegisterCleanup registers a cleanup function
func (rcm *ResourceCleanupManager) RegisterCleanup(cleanup func() error) {
	rcm.mutex.Lock()
	defer rcm.mutex.Unlock()
	rcm.managers = append(rcm.managers, cleanup)
}

// CleanupAll executes all registered cleanup functions
func (rcm *ResourceCleanupManager) CleanupAll() error {
	rcm.mutex.Lock()
	defer rcm.mutex.Unlock()

	var lastError error
	for _, cleanup := range rcm.managers {
		if err := cleanup(); err != nil {
			lastError = err
		}
	}

	// Clear the cleanup functions
	rcm.managers = rcm.managers[:0]
	return lastError
}

// ForceGarbageCollection forces garbage collection and waits for it to complete
func ForceGarbageCollection() {
	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
}

// GetMemoryStats returns current memory statistics
func GetMemoryStats() map[string]uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return map[string]uint64{
		"alloc":         memStats.Alloc,
		"total_alloc":   memStats.TotalAlloc,
		"sys":           memStats.Sys,
		"heap_alloc":    memStats.HeapAlloc,
		"heap_sys":      memStats.HeapSys,
		"heap_idle":     memStats.HeapIdle,
		"heap_inuse":    memStats.HeapInuse,
		"heap_released": memStats.HeapReleased,
		"heap_objects":  memStats.HeapObjects,
		"stack_inuse":   memStats.StackInuse,
		"stack_sys":     memStats.StackSys,
		"gc_cycles":     uint64(memStats.NumGC),
	}
}

// GetGoroutineCount returns the current number of goroutines
func GetGoroutineCount() int {
	return runtime.NumGoroutine()
}

// LogResourceUsage logs current resource usage
func LogResourceUsage(prefix string) {
	memStats := GetMemoryStats()
	goroutines := GetGoroutineCount()

	fmt.Printf("%s Resource Usage:\n", prefix)
	fmt.Printf("  Goroutines: %d\n", goroutines)
	fmt.Printf("  Memory Alloc: %d bytes\n", memStats["alloc"])
	fmt.Printf("  Memory Sys: %d bytes\n", memStats["sys"])
	fmt.Printf("  Heap Objects: %d\n", memStats["heap_objects"])
	fmt.Printf("  GC Cycles: %d\n", memStats["gc_cycles"])
}
