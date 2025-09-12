package otel

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// OTELManager manages OpenTelemetry providers and configuration
type OTELManager struct {
	config          *OTELConfig
	tracer          oteltrace.Tracer
	meter           metric.Meter
	tracerProvider  *sdktrace.TracerProvider
	meterProvider   *sdkmetric.MeterProvider
	exporterManager *ExporterManager
	shutdown        func(context.Context) error
	initialized     bool
}

// NewOTELManager creates a new OTEL manager with the given configuration
func NewOTELManager(config *OTELConfig) (*OTELManager, error) {
	if config == nil {
		config = DefaultOTELConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid OTEL configuration: %w", err)
	}

	manager := &OTELManager{
		config:          config,
		exporterManager: NewExporterManager(),
		initialized:     false,
	}

	return manager, nil
}

// Initialize sets up the OpenTelemetry providers based on configuration
func (m *OTELManager) Initialize(ctx context.Context) error {
	if m.initialized {
		return nil
	}

	// Register with global resource manager for cleanup
	RegisterGlobalManager(m)

	if !m.config.Enabled {
		// Use no-op providers when disabled
		m.tracer = otel.Tracer(m.config.ServiceName)
		m.meter = otel.Meter(m.config.ServiceName)
		m.shutdown = func(context.Context) error { return nil }
		m.initialized = true
		return nil
	}

	// Create resource with service information and custom attributes
	res, err := m.createResource()
	if err != nil {
		return NewError("resource creation", err)
	}

	// Initialize tracer provider
	if err := m.initializeTracerProvider(ctx, res); err != nil {
		return NewError("tracer provider initialization", err)
	}

	// Initialize meter provider
	if err := m.initializeMeterProvider(ctx, res); err != nil {
		// Clean up tracer provider if meter provider fails
		if m.tracerProvider != nil {
			m.tracerProvider.Shutdown(ctx)
		}
		return NewError("meter provider initialization", err)
	}

	// Get tracer and meter instances
	m.tracer = m.tracerProvider.Tracer(m.config.ServiceName)
	m.meter = m.meterProvider.Meter(m.config.ServiceName)

	// Set up shutdown function
	m.shutdown = func(shutdownCtx context.Context) error {
		var errs []error

		if m.tracerProvider != nil {
			if err := m.tracerProvider.Shutdown(shutdownCtx); err != nil {
				errs = append(errs, fmt.Errorf("tracer provider shutdown: %w", err))
			}
		}

		if m.meterProvider != nil {
			if err := m.meterProvider.Shutdown(shutdownCtx); err != nil {
				errs = append(errs, fmt.Errorf("meter provider shutdown: %w", err))
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("shutdown errors: %v", errs)
		}
		return nil
	}

	m.initialized = true
	log.Printf("OTEL Manager initialized with service name: %s", m.config.ServiceName)
	return nil
}

// GetTracer returns the configured tracer
func (m *OTELManager) GetTracer() oteltrace.Tracer {
	if !m.initialized {
		log.Printf("Warning: OTEL Manager not initialized, returning no-op tracer")
		return otel.Tracer(m.config.ServiceName)
	}
	return m.tracer
}

// GetMeter returns the configured meter
func (m *OTELManager) GetMeter() metric.Meter {
	if !m.initialized {
		log.Printf("Warning: OTEL Manager not initialized, returning no-op meter")
		return otel.Meter(m.config.ServiceName)
	}
	return m.meter
}

// GetConfig returns the current configuration
func (m *OTELManager) GetConfig() *OTELConfig {
	return m.config
}

// IsEnabled returns whether OTEL is enabled
func (m *OTELManager) IsEnabled() bool {
	return m.config.Enabled
}

// IsInitialized returns whether the manager has been initialized
func (m *OTELManager) IsInitialized() bool {
	return m.initialized
}

// Shutdown gracefully shuts down the OTEL providers
func (m *OTELManager) Shutdown(ctx context.Context) error {
	if !m.initialized {
		return nil
	}

	// Unregister from global resource manager
	UnregisterGlobalManager(m)

	if m.shutdown != nil {
		if err := m.shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown OTEL providers: %w", err)
		}
	}

	m.initialized = false
	log.Printf("OTEL Manager shutdown completed")
	return nil
}

// GetExportStats returns statistics about export operations
func (m *OTELManager) GetExportStats() map[string]interface{} {
	if !m.initialized || m.exporterManager == nil {
		return map[string]interface{}{
			"initialized": false,
		}
	}

	stats := m.exporterManager.GetAllStats()
	stats["initialized"] = true
	stats["enabled"] = m.config.Enabled
	return stats
}

// LogExportStats logs export statistics
func (m *OTELManager) LogExportStats() {
	if !m.initialized || m.exporterManager == nil {
		log.Printf("OTEL Manager not initialized, no export stats available")
		return
	}

	m.exporterManager.LogStats()
}

// ResetExportFailures resets export failure counters and circuit breakers
func (m *OTELManager) ResetExportFailures() {
	if !m.initialized || m.exporterManager == nil {
		log.Printf("OTEL Manager not initialized, cannot reset export failures")
		return
	}

	m.exporterManager.Reset()
	log.Printf("OTEL export failures reset")
}

// createResource creates an OpenTelemetry resource with service information
func (m *OTELManager) createResource() (*resource.Resource, error) {
	// Start with default service attributes
	attrs := []attribute.KeyValue{
		semconv.ServiceName(m.config.ServiceName),
	}

	// Add service version if provided
	if m.config.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(m.config.ServiceVersion))
	}

	// Add custom resource attributes
	for key, value := range m.config.ResourceAttrs {
		attrs = append(attrs, attribute.String(key, value))
	}

	// Create resource with default attributes merged with custom ones
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			attrs...,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	return res, nil
}

// initializeTracerProvider sets up the tracer provider with OTLP exporter
func (m *OTELManager) initializeTracerProvider(ctx context.Context, res *resource.Resource) error {
	// Create tracer provider options
	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(res))

	// Add sampling if configured
	if m.config.SamplingRate < 1.0 {
		sampler := sdktrace.TraceIDRatioBased(m.config.SamplingRate)
		opts = append(opts, sdktrace.WithSampler(sampler))
	}

	// Create and configure trace exporter if endpoint is provided
	if m.config.TraceEndpoint != "" {
		exporter, err := m.createTraceExporter(ctx)
		if err != nil {
			return fmt.Errorf("failed to create trace exporter: %w", err)
		}

		// Use batch span processor for better performance
		batchProcessor := sdktrace.NewBatchSpanProcessor(
			exporter,
			sdktrace.WithBatchTimeout(m.config.BatchTimeout),
			sdktrace.WithMaxExportBatchSize(m.config.BatchSize),
		)
		opts = append(opts, sdktrace.WithSpanProcessor(batchProcessor))
	}

	// Create tracer provider
	m.tracerProvider = sdktrace.NewTracerProvider(opts...)

	// Set as global tracer provider
	otel.SetTracerProvider(m.tracerProvider)

	return nil
}

// initializeMeterProvider sets up the meter provider with OTLP exporter
func (m *OTELManager) initializeMeterProvider(ctx context.Context, res *resource.Resource) error {
	// Create meter provider options
	var opts []sdkmetric.Option
	opts = append(opts, sdkmetric.WithResource(res))

	// Create and configure metric exporter if endpoint is provided
	if m.config.MetricEndpoint != "" {
		exporter, err := m.createMetricExporter(ctx)
		if err != nil {
			return fmt.Errorf("failed to create metric exporter: %w", err)
		}

		// Use periodic reader for regular metric export
		reader := sdkmetric.NewPeriodicReader(
			exporter,
			sdkmetric.WithInterval(m.config.BatchTimeout),
		)
		opts = append(opts, sdkmetric.WithReader(reader))
	}

	// Create meter provider
	m.meterProvider = sdkmetric.NewMeterProvider(opts...)

	// Set as global meter provider
	otel.SetMeterProvider(m.meterProvider)

	return nil
}

// createTraceExporter creates an OTLP trace exporter with retry and circuit breaker logic
func (m *OTELManager) createTraceExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	// Prepare exporter options
	var opts []otlptracehttp.Option

	// Set endpoint
	opts = append(opts, otlptracehttp.WithEndpoint(m.config.TraceEndpoint))

	// Add headers if configured
	if len(m.config.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(m.config.Headers))
	}

	// Configure TLS settings
	if m.config.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	// Set timeout for export operations
	opts = append(opts, otlptracehttp.WithTimeout(m.config.ExportTimeout))

	// Configure compression
	switch strings.ToLower(m.config.Compression) {
	case "gzip":
		opts = append(opts, otlptracehttp.WithCompression(otlptracehttp.GzipCompression))
	case "none":
		opts = append(opts, otlptracehttp.WithCompression(otlptracehttp.NoCompression))
	default:
		// Default to gzip for better performance
		opts = append(opts, otlptracehttp.WithCompression(otlptracehttp.GzipCompression))
	}

	// Create base exporter
	baseExporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, NewError("trace exporter creation",
			fmt.Errorf("failed to create OTLP trace exporter with endpoint %s: %w",
				m.config.TraceEndpoint, err))
	}

	// Wrap with instrumented exporter for retry and circuit breaker logic
	instrumentedExporter := NewInstrumentedTraceExporterWithConfig(baseExporter, m.config.ServiceName, m.config)
	m.exporterManager.SetTraceExporter(instrumentedExporter)

	log.Printf("OTLP trace exporter created for endpoint: %s", m.config.TraceEndpoint)
	return instrumentedExporter, nil
}

// createMetricExporter creates an OTLP metric exporter with retry and circuit breaker logic
func (m *OTELManager) createMetricExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	// Prepare exporter options
	var opts []otlpmetrichttp.Option

	// Set endpoint
	opts = append(opts, otlpmetrichttp.WithEndpoint(m.config.MetricEndpoint))

	// Add headers if configured
	if len(m.config.Headers) > 0 {
		opts = append(opts, otlpmetrichttp.WithHeaders(m.config.Headers))
	}

	// Configure TLS settings
	if m.config.Insecure {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}

	// Set timeout for export operations
	opts = append(opts, otlpmetrichttp.WithTimeout(m.config.ExportTimeout))

	// Configure compression
	switch strings.ToLower(m.config.Compression) {
	case "gzip":
		opts = append(opts, otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression))
	case "none":
		opts = append(opts, otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression))
	default:
		// Default to gzip for better performance
		opts = append(opts, otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression))
	}

	// Create base exporter
	baseExporter, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		return nil, NewError("metric exporter creation",
			fmt.Errorf("failed to create OTLP metric exporter with endpoint %s: %w",
				m.config.MetricEndpoint, err))
	}

	// Wrap with instrumented exporter for retry and circuit breaker logic
	instrumentedExporter := NewInstrumentedMetricExporterWithConfig(baseExporter, m.config.ServiceName, m.config)
	m.exporterManager.SetMetricExporter(instrumentedExporter)

	log.Printf("OTLP metric exporter created for endpoint: %s", m.config.MetricEndpoint)
	return instrumentedExporter, nil
}
