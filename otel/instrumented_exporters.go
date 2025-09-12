package otel

import (
	"context"
	"log"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
)

// InstrumentedTraceExporter wraps a trace exporter with retry and circuit breaker logic
type InstrumentedTraceExporter struct {
	exporter       trace.SpanExporter
	failureHandler *ExportFailureHandler
	serviceName    string
}

// NewInstrumentedTraceExporter creates a new instrumented trace exporter
func NewInstrumentedTraceExporter(exporter trace.SpanExporter, serviceName string) *InstrumentedTraceExporter {
	return &InstrumentedTraceExporter{
		exporter:       exporter,
		failureHandler: NewExportFailureHandler(nil, nil),
		serviceName:    serviceName,
	}
}

// NewInstrumentedTraceExporterWithConfig creates a new instrumented trace exporter with custom configuration
func NewInstrumentedTraceExporterWithConfig(exporter trace.SpanExporter, serviceName string, config *OTELConfig) *InstrumentedTraceExporter {
	var retryConfig *RetryConfig
	var cbConfig *CircuitBreakerConfig

	if config != nil {
		retryConfig = &RetryConfig{
			MaxAttempts:   config.MaxRetryAttempts,
			InitialDelay:  config.RetryDelay,
			MaxDelay:      config.RetryMaxDelay,
			BackoffFactor: config.RetryBackoffFactor,
			Jitter:        config.RetryJitter,
		}
		if !config.RetryEnabled {
			// Honor RetryEnabled=false by forcing single-attempt behavior
			retryConfig.MaxAttempts = 1
		}

		if config.CircuitBreakerEnabled {
			cbConfig = &CircuitBreakerConfig{
				FailureThreshold: config.CircuitFailureThreshold,
				RecoveryTimeout:  config.CircuitRecoveryTimeout,
				HalfOpenMaxCalls: config.CircuitHalfOpenMaxCalls,
			}
		}
	}

	return &InstrumentedTraceExporter{
		exporter:       exporter,
		failureHandler: NewExportFailureHandler(retryConfig, cbConfig),
		serviceName:    serviceName,
	}
}

// ExportSpans exports spans with retry and circuit breaker protection
func (e *InstrumentedTraceExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	operation := "trace_export"

	return e.failureHandler.HandleExport(ctx, operation, func() error {
		return e.exporter.ExportSpans(ctx, spans)
	})
}

// Shutdown shuts down the underlying exporter
func (e *InstrumentedTraceExporter) Shutdown(ctx context.Context) error {
	operation := "trace_export_shutdown"

	return e.failureHandler.HandleExport(ctx, operation, func() error {
		return e.exporter.Shutdown(ctx)
	})
}

// GetStats returns export statistics
func (e *InstrumentedTraceExporter) GetStats() map[string]interface{} {
	stats := e.failureHandler.GetStats()
	stats["exporter_type"] = "trace"
	stats["service_name"] = e.serviceName
	return stats
}

// Reset resets the failure handler state
func (e *InstrumentedTraceExporter) Reset() {
	e.failureHandler.Reset()
}

// InstrumentedMetricExporter wraps a metric exporter with retry and circuit breaker logic
type InstrumentedMetricExporter struct {
	exporter       metric.Exporter
	failureHandler *ExportFailureHandler
	serviceName    string
}

// NewInstrumentedMetricExporter creates a new instrumented metric exporter
func NewInstrumentedMetricExporter(exporter metric.Exporter, serviceName string) *InstrumentedMetricExporter {
	return &InstrumentedMetricExporter{
		exporter:       exporter,
		failureHandler: NewExportFailureHandler(nil, nil),
		serviceName:    serviceName,
	}
}

// NewInstrumentedMetricExporterWithConfig creates a new instrumented metric exporter with custom configuration
func NewInstrumentedMetricExporterWithConfig(exporter metric.Exporter, serviceName string, config *OTELConfig) *InstrumentedMetricExporter {
	var retryConfig *RetryConfig
	var cbConfig *CircuitBreakerConfig

	if config != nil {
		retryConfig = &RetryConfig{
			MaxAttempts:   config.MaxRetryAttempts,
			InitialDelay:  config.RetryDelay,
			MaxDelay:      config.RetryMaxDelay,
			BackoffFactor: config.RetryBackoffFactor,
			Jitter:        config.RetryJitter,
		}
		if !config.RetryEnabled {
			retryConfig.MaxAttempts = 1
		}

		if config.CircuitBreakerEnabled {
			cbConfig = &CircuitBreakerConfig{
				FailureThreshold: config.CircuitFailureThreshold,
				RecoveryTimeout:  config.CircuitRecoveryTimeout,
				HalfOpenMaxCalls: config.CircuitHalfOpenMaxCalls,
			}
		}
	}

	return &InstrumentedMetricExporter{
		exporter:       exporter,
		failureHandler: NewExportFailureHandler(retryConfig, cbConfig),
		serviceName:    serviceName,
	}
}

// Export exports metrics with retry and circuit breaker protection
func (e *InstrumentedMetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	operation := "metric_export"

	return e.failureHandler.HandleExport(ctx, operation, func() error {
		return e.exporter.Export(ctx, rm)
	})
}

// Temporality returns the Temporality to use for an instrument kind
func (e *InstrumentedMetricExporter) Temporality(kind metric.InstrumentKind) metricdata.Temporality {
	return e.exporter.Temporality(kind)
}

// Aggregation returns the Aggregation to use for an instrument kind
func (e *InstrumentedMetricExporter) Aggregation(kind metric.InstrumentKind) metric.Aggregation {
	return e.exporter.Aggregation(kind)
}

// ForceFlush forces a flush of the underlying exporter
func (e *InstrumentedMetricExporter) ForceFlush(ctx context.Context) error {
	operation := "metric_export_flush"

	return e.failureHandler.HandleExport(ctx, operation, func() error {
		return e.exporter.ForceFlush(ctx)
	})
}

// Shutdown shuts down the underlying exporter
func (e *InstrumentedMetricExporter) Shutdown(ctx context.Context) error {
	operation := "metric_export_shutdown"

	return e.failureHandler.HandleExport(ctx, operation, func() error {
		return e.exporter.Shutdown(ctx)
	})
}

// GetStats returns export statistics
func (e *InstrumentedMetricExporter) GetStats() map[string]interface{} {
	stats := e.failureHandler.GetStats()
	stats["exporter_type"] = "metric"
	stats["service_name"] = e.serviceName
	return stats
}

// Reset resets the failure handler state
func (e *InstrumentedMetricExporter) Reset() {
	e.failureHandler.Reset()
}

// ExporterManager manages instrumented exporters and provides centralized statistics
type ExporterManager struct {
	traceExporter  *InstrumentedTraceExporter
	metricExporter *InstrumentedMetricExporter
}

// NewExporterManager creates a new exporter manager
func NewExporterManager() *ExporterManager {
	return &ExporterManager{}
}

// SetTraceExporter sets the trace exporter
func (em *ExporterManager) SetTraceExporter(exporter *InstrumentedTraceExporter) {
	em.traceExporter = exporter
}

// SetMetricExporter sets the metric exporter
func (em *ExporterManager) SetMetricExporter(exporter *InstrumentedMetricExporter) {
	em.metricExporter = exporter
}

// GetTraceExporter returns the trace exporter
func (em *ExporterManager) GetTraceExporter() *InstrumentedTraceExporter {
	return em.traceExporter
}

// GetMetricExporter returns the metric exporter
func (em *ExporterManager) GetMetricExporter() *InstrumentedMetricExporter {
	return em.metricExporter
}

// GetAllStats returns statistics from all exporters
func (em *ExporterManager) GetAllStats() map[string]interface{} {
	stats := make(map[string]interface{})

	if em.traceExporter != nil {
		stats["trace"] = em.traceExporter.GetStats()
	}

	if em.metricExporter != nil {
		stats["metric"] = em.metricExporter.GetStats()
	}

	return stats
}

// Reset resets all exporters
func (em *ExporterManager) Reset() {
	if em.traceExporter != nil {
		em.traceExporter.Reset()
	}

	if em.metricExporter != nil {
		em.metricExporter.Reset()
	}

	log.Printf("All OTEL exporters reset")
}

// LogStats logs statistics from all exporters
func (em *ExporterManager) LogStats() {
	stats := em.GetAllStats()

	for exporterType, exporterStats := range stats {
		if statsMap, ok := exporterStats.(map[string]interface{}); ok {
			log.Printf("OTEL %s exporter stats: %+v", exporterType, statsMap)
		}
	}
}
