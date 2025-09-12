package otel

import (
	"context"
	"fmt"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// OTELManager manages OpenTelemetry providers and configuration
type OTELManager struct {
	config      *OTELConfig
	tracer      trace.Tracer
	meter       metric.Meter
	shutdown    func(context.Context) error
	initialized bool
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
		config:      config,
		initialized: false,
	}

	return manager, nil
}

// Initialize sets up the OpenTelemetry providers based on configuration
func (m *OTELManager) Initialize(ctx context.Context) error {
	if m.initialized {
		return nil
	}

	if !m.config.Enabled {
		// Use no-op providers when disabled
		m.tracer = otel.Tracer(m.config.ServiceName)
		m.meter = otel.Meter(m.config.ServiceName)
		m.shutdown = func(context.Context) error { return nil }
		m.initialized = true
		return nil
	}

	// TODO: Initialize actual providers with exporters in task 2
	// For now, use global providers as placeholders
	m.tracer = otel.Tracer(m.config.ServiceName)
	m.meter = otel.Meter(m.config.ServiceName)
	m.shutdown = func(context.Context) error { return nil }
	m.initialized = true

	log.Printf("OTEL Manager initialized with service name: %s", m.config.ServiceName)
	return nil
}

// GetTracer returns the configured tracer
func (m *OTELManager) GetTracer() trace.Tracer {
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

	if m.shutdown != nil {
		if err := m.shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown OTEL providers: %w", err)
		}
	}

	m.initialized = false
	log.Printf("OTEL Manager shutdown completed")
	return nil
}
