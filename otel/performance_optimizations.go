package otel

import (
	"context"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// AttributePool provides a pool of reusable attribute slices to reduce allocations
type AttributePool struct {
	pool sync.Pool
}

// NewAttributePool creates a new attribute pool
func NewAttributePool() *AttributePool {
	return &AttributePool{
		pool: sync.Pool{
			New: func() interface{} {
				// Pre-allocate slice with capacity for common use cases
				return make([]attribute.KeyValue, 0, 8)
			},
		},
	}
}

// Get retrieves an attribute slice from the pool
func (p *AttributePool) Get() []attribute.KeyValue {
	return p.pool.Get().([]attribute.KeyValue)
}

// Put returns an attribute slice to the pool
func (p *AttributePool) Put(attrs []attribute.KeyValue) {
	// Clear the slice but keep the capacity
	attrs = attrs[:0]
	p.pool.Put(attrs)
}

// Global attribute pool for reuse across the application
var globalAttributePool = NewAttributePool()

// OptimizedSpanCreator provides optimized span creation with minimal allocations
type OptimizedSpanCreator struct {
	tracer trace.Tracer
	pool   *AttributePool
}

// NewOptimizedSpanCreator creates a new optimized span creator
func NewOptimizedSpanCreator(tracer trace.Tracer) *OptimizedSpanCreator {
	return &OptimizedSpanCreator{
		tracer: tracer,
		pool:   globalAttributePool,
	}
}

// CreateSpan creates a span with optimized attribute handling
func (osc *OptimizedSpanCreator) CreateSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	// Use the tracer directly for span creation (no additional overhead)
	return osc.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// CreateSpanWithBuilder creates a span using a builder pattern for complex attributes
func (osc *OptimizedSpanCreator) CreateSpanWithBuilder(ctx context.Context, name string, builder func([]attribute.KeyValue) []attribute.KeyValue) (context.Context, trace.Span) {
	// Get attribute slice from pool
	attrs := osc.pool.Get()
	defer osc.pool.Put(attrs)

	// Build attributes
	attrs = builder(attrs)

	// Create span
	return osc.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// OptimizedMetricRecorder provides optimized metric recording with minimal allocations
type OptimizedMetricRecorder struct {
	meter metric.Meter
	pool  *AttributePool

	// Pre-created instruments to avoid repeated creation
	counters   sync.Map // map[string]metric.Int64Counter
	histograms sync.Map // map[string]metric.Float64Histogram
	gauges     sync.Map // map[string]metric.Int64UpDownCounter
}

// NewOptimizedMetricRecorder creates a new optimized metric recorder
func NewOptimizedMetricRecorder(meter metric.Meter) *OptimizedMetricRecorder {
	return &OptimizedMetricRecorder{
		meter: meter,
		pool:  globalAttributePool,
	}
}

// GetOrCreateCounter gets or creates a counter instrument
func (omr *OptimizedMetricRecorder) GetOrCreateCounter(name string, description string) (metric.Int64Counter, error) {
	if counter, ok := omr.counters.Load(name); ok {
		return counter.(metric.Int64Counter), nil
	}

	counter, err := omr.meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		return nil, err
	}

	omr.counters.Store(name, counter)
	return counter, nil
}

// GetOrCreateHistogram gets or creates a histogram instrument
func (omr *OptimizedMetricRecorder) GetOrCreateHistogram(name string, description string, unit string) (metric.Float64Histogram, error) {
	if histogram, ok := omr.histograms.Load(name); ok {
		return histogram.(metric.Float64Histogram), nil
	}

	histogram, err := omr.meter.Float64Histogram(name,
		metric.WithDescription(description),
		metric.WithUnit(unit))
	if err != nil {
		return nil, err
	}

	omr.histograms.Store(name, histogram)
	return histogram, nil
}

// GetOrCreateGauge gets or creates a gauge instrument
func (omr *OptimizedMetricRecorder) GetOrCreateGauge(name string, description string) (metric.Int64UpDownCounter, error) {
	if gauge, ok := omr.gauges.Load(name); ok {
		return gauge.(metric.Int64UpDownCounter), nil
	}

	gauge, err := omr.meter.Int64UpDownCounter(name, metric.WithDescription(description))
	if err != nil {
		return nil, err
	}

	omr.gauges.Store(name, gauge)
	return gauge, nil
}

// RecordCounterWithBuilder records a counter metric using a builder pattern
func (omr *OptimizedMetricRecorder) RecordCounterWithBuilder(ctx context.Context, name string, value int64, builder func([]attribute.KeyValue) []attribute.KeyValue) error {
	counter, err := omr.GetOrCreateCounter(name, "")
	if err != nil {
		return err
	}

	// Get attribute slice from pool
	attrs := omr.pool.Get()
	defer omr.pool.Put(attrs)

	// Build attributes
	attrs = builder(attrs)

	// Record metric
	counter.Add(ctx, value, metric.WithAttributes(attrs...))
	return nil
}

// RecordHistogramWithBuilder records a histogram metric using a builder pattern
func (omr *OptimizedMetricRecorder) RecordHistogramWithBuilder(ctx context.Context, name string, value float64, unit string, builder func([]attribute.KeyValue) []attribute.KeyValue) error {
	histogram, err := omr.GetOrCreateHistogram(name, "", unit)
	if err != nil {
		return err
	}

	// Get attribute slice from pool
	attrs := omr.pool.Get()
	defer omr.pool.Put(attrs)

	// Build attributes
	attrs = builder(attrs)

	// Record metric
	histogram.Record(ctx, value, metric.WithAttributes(attrs...))
	return nil
}

// PerformanceMonitor monitors the performance impact of OTEL operations
type PerformanceMonitor struct {
	enabled bool
	stats   sync.Map // map[string]*OperationStats
}

// OperationStats holds statistics for an operation
type OperationStats struct {
	Count     int64
	TotalTime time.Duration
	MinTime   time.Duration
	MaxTime   time.Duration
	LastTime  time.Duration
	mutex     sync.RWMutex
}

// newOperationStats creates a new OperationStats with proper initialization
func newOperationStats(initialDuration time.Duration) *OperationStats {
	return &OperationStats{
		Count:     0,
		TotalTime: 0,
		MinTime:   0, // Will be set on first record
		MaxTime:   0, // Will be set on first record
		LastTime:  0,
		mutex:     sync.RWMutex{},
	}
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor(enabled bool) *PerformanceMonitor {
	return &PerformanceMonitor{
		enabled: enabled,
	}
}

// MeasureOperation measures the performance of an operation
func (pm *PerformanceMonitor) MeasureOperation(name string, fn func()) {
	if !pm.enabled {
		fn()
		return
	}

	start := time.Now()
	fn()
	duration := time.Since(start)

	pm.recordStats(name, duration)
}

// recordStats records statistics for an operation
func (pm *PerformanceMonitor) recordStats(name string, duration time.Duration) {
	// Use LoadOrStore with proper constructor to avoid race conditions
	statsInterface, _ := pm.stats.LoadOrStore(name, newOperationStats(duration))
	stats := statsInterface.(*OperationStats)

	stats.mutex.Lock()
	defer stats.mutex.Unlock()

	// Always increment and update - the logic is the same whether it's new or existing
	stats.Count++
	stats.TotalTime += duration
	stats.LastTime = duration

	// Update min/max times
	if stats.Count == 1 {
		// First record - initialize min/max
		stats.MinTime = duration
		stats.MaxTime = duration
	} else {
		// Subsequent records - update min/max
		if duration < stats.MinTime {
			stats.MinTime = duration
		}
		if duration > stats.MaxTime {
			stats.MaxTime = duration
		}
	}
}

// GetStats returns statistics for all operations
func (pm *PerformanceMonitor) GetStats() map[string]OperationStats {
	result := make(map[string]OperationStats)
	pm.stats.Range(func(key, value interface{}) bool {
		name := key.(string)
		stats := value.(*OperationStats)

		stats.mutex.RLock()
		result[name] = *stats
		stats.mutex.RUnlock()

		return true
	})
	return result
}

// Reset resets all statistics
func (pm *PerformanceMonitor) Reset() {
	pm.stats = sync.Map{}
}

// ResourceManager manages OTEL resources and provides cleanup utilities
type ResourceManager struct {
	managers    []*OTELManager
	mutex       sync.RWMutex
	cleanupDone chan struct{}
	stopCleanup chan struct{}
}

// NewResourceManager creates a new resource manager
func NewResourceManager() *ResourceManager {
	rm := &ResourceManager{
		cleanupDone: make(chan struct{}),
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup routine
	go rm.backgroundCleanup()

	return rm
}

// RegisterManager registers an OTEL manager for resource management
func (rm *ResourceManager) RegisterManager(manager *OTELManager) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()
	rm.managers = append(rm.managers, manager)
}

// UnregisterManager unregisters an OTEL manager
func (rm *ResourceManager) UnregisterManager(manager *OTELManager) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	for i, m := range rm.managers {
		if m == manager {
			rm.managers = append(rm.managers[:i], rm.managers[i+1:]...)
			break
		}
	}
}

// ShutdownAll shuts down all registered managers
func (rm *ResourceManager) ShutdownAll(ctx context.Context) error {
	rm.mutex.RLock()
	managers := make([]*OTELManager, len(rm.managers))
	copy(managers, rm.managers)
	rm.mutex.RUnlock()

	var lastError error
	for _, manager := range managers {
		if err := manager.Shutdown(ctx); err != nil {
			lastError = err
		}
	}

	return lastError
}

// GetResourceUsage returns current resource usage statistics
func (rm *ResourceManager) GetResourceUsage() map[string]interface{} {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	rm.mutex.RLock()
	managerCount := len(rm.managers)
	rm.mutex.RUnlock()

	return map[string]interface{}{
		"goroutines":     runtime.NumGoroutine(),
		"memory_alloc":   memStats.Alloc,
		"memory_sys":     memStats.Sys,
		"gc_cycles":      memStats.NumGC,
		"managers_count": managerCount,
	}
}

// backgroundCleanup performs periodic cleanup tasks
func (rm *ResourceManager) backgroundCleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.performCleanup()
		case <-rm.stopCleanup:
			close(rm.cleanupDone)
			return
		}
	}
}

// performCleanup performs cleanup tasks
func (rm *ResourceManager) performCleanup() {
	// Force garbage collection periodically to prevent memory buildup
	runtime.GC()

	// Log resource usage if any managers are active
	rm.mutex.RLock()
	managerCount := len(rm.managers)
	rm.mutex.RUnlock()

	if managerCount > 0 {
		usage := rm.GetResourceUsage()
		// Only log if resource usage is high
		if usage["goroutines"].(int) > 100 || usage["memory_alloc"].(uint64) > 100*1024*1024 {
			// Log high resource usage (implementation would depend on logging framework)
		}
	}
}

// Stop stops the resource manager
func (rm *ResourceManager) Stop() {
	close(rm.stopCleanup)
	<-rm.cleanupDone
}

// Global resource manager instance
var globalResourceManager = NewResourceManager()

// RegisterGlobalManager registers a manager with the global resource manager
func RegisterGlobalManager(manager *OTELManager) {
	globalResourceManager.RegisterManager(manager)
}

// UnregisterGlobalManager unregisters a manager from the global resource manager
func UnregisterGlobalManager(manager *OTELManager) {
	globalResourceManager.UnregisterManager(manager)
}

// GetGlobalResourceUsage returns global resource usage statistics
func GetGlobalResourceUsage() map[string]interface{} {
	return globalResourceManager.GetResourceUsage()
}

// ShutdownAllGlobalManagers shuts down all globally registered managers
func ShutdownAllGlobalManagers(ctx context.Context) error {
	return globalResourceManager.ShutdownAll(ctx)
}

// OptimizedAttributeBuilder provides a fluent interface for building attributes efficiently
type OptimizedAttributeBuilder struct {
	attrs []attribute.KeyValue
	pool  *AttributePool
}

// NewOptimizedAttributeBuilder creates a new optimized attribute builder
func NewOptimizedAttributeBuilder() *OptimizedAttributeBuilder {
	return &OptimizedAttributeBuilder{
		attrs: globalAttributePool.Get(),
		pool:  globalAttributePool,
	}
}

// String adds a string attribute
func (oab *OptimizedAttributeBuilder) String(key, value string) *OptimizedAttributeBuilder {
	oab.attrs = append(oab.attrs, attribute.String(key, value))
	return oab
}

// Int adds an int attribute
func (oab *OptimizedAttributeBuilder) Int(key string, value int) *OptimizedAttributeBuilder {
	oab.attrs = append(oab.attrs, attribute.Int(key, value))
	return oab
}

// Int64 adds an int64 attribute
func (oab *OptimizedAttributeBuilder) Int64(key string, value int64) *OptimizedAttributeBuilder {
	oab.attrs = append(oab.attrs, attribute.Int64(key, value))
	return oab
}

// Float64 adds a float64 attribute
func (oab *OptimizedAttributeBuilder) Float64(key string, value float64) *OptimizedAttributeBuilder {
	oab.attrs = append(oab.attrs, attribute.Float64(key, value))
	return oab
}

// Bool adds a bool attribute
func (oab *OptimizedAttributeBuilder) Bool(key string, value bool) *OptimizedAttributeBuilder {
	oab.attrs = append(oab.attrs, attribute.Bool(key, value))
	return oab
}

// StringSlice adds a string slice attribute
func (oab *OptimizedAttributeBuilder) StringSlice(key string, value []string) *OptimizedAttributeBuilder {
	oab.attrs = append(oab.attrs, attribute.StringSlice(key, value))
	return oab
}

// Build returns the built attributes and returns the slice to the pool
func (oab *OptimizedAttributeBuilder) Build() []attribute.KeyValue {
	// Make a copy of the attributes
	result := make([]attribute.KeyValue, len(oab.attrs))
	copy(result, oab.attrs)

	// Return the slice to the pool
	oab.pool.Put(oab.attrs)
	oab.attrs = nil

	return result
}

// BuildAndApply applies the attributes to a function and then cleans up
func (oab *OptimizedAttributeBuilder) BuildAndApply(fn func([]attribute.KeyValue)) {
	defer oab.pool.Put(oab.attrs)
	fn(oab.attrs)
	oab.attrs = nil
}
