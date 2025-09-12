package otel

import (
	"context"
	"testing"
	"time"
)

func TestDefaultOTELConfig(t *testing.T) {
	config := DefaultOTELConfig()
	
	if config.Enabled {
		t.Error("Expected default config to have OTEL disabled")
	}
	
	if config.ServiceName != "tdcli" {
		t.Errorf("Expected service name 'tdcli', got '%s'", config.ServiceName)
	}
	
	if config.SamplingRate != 1.0 {
		t.Errorf("Expected sampling rate 1.0, got %f", config.SamplingRate)
	}
	
	if config.BatchTimeout != 5*time.Second {
		t.Errorf("Expected batch timeout 5s, got %v", config.BatchTimeout)
	}
}

func TestOTELConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *OTELConfig
		wantErr bool
	}{
		{
			name:    "disabled config should be valid",
			config:  &OTELConfig{Enabled: false},
			wantErr: false,
		},
		{
			name: "valid enabled config",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test-service",
				SamplingRate:  0.5,
				BatchTimeout:  1 * time.Second,
				BatchSize:     100,
				ExportTimeout: 30 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "empty service name should fail",
			config: &OTELConfig{
				Enabled:     true,
				ServiceName: "",
			},
			wantErr: true,
		},
		{
			name: "invalid sampling rate should fail",
			config: &OTELConfig{
				Enabled:      true,
				ServiceName:  "test",
				SamplingRate: 1.5,
			},
			wantErr: true,
		},
		{
			name: "invalid endpoint should fail",
			config: &OTELConfig{
				Enabled:       true,
				ServiceName:   "test",
				SamplingRate:  1.0,
				TraceEndpoint: "invalid-url",
				BatchTimeout:  1 * time.Second,
				BatchSize:     100,
				ExportTimeout: 30 * time.Second,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewOTELManager(t *testing.T) {
	config := DefaultOTELConfig()
	
	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}
	
	if manager.IsInitialized() {
		t.Error("Expected manager to not be initialized yet")
	}
	
	if manager.IsEnabled() {
		t.Error("Expected manager to be disabled with default config")
	}
}

func TestOTELManagerInitialization(t *testing.T) {
	config := DefaultOTELConfig()
	config.Enabled = false // Explicitly disabled
	
	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}
	
	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	
	if !manager.IsInitialized() {
		t.Error("Expected manager to be initialized")
	}
	
	tracer := manager.GetTracer()
	if tracer == nil {
		t.Error("Expected tracer to be available")
	}
	
	meter := manager.GetMeter()
	if meter == nil {
		t.Error("Expected meter to be available")
	}
	
	// Test shutdown
	err = manager.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown OTEL manager: %v", err)
	}
	
	if manager.IsInitialized() {
		t.Error("Expected manager to not be initialized after shutdown")
	}
}
func TestOTELManagerWithEnabledConfig(t *testing.T) {
	config := &OTELConfig{
		Enabled:        true,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		SamplingRate:   0.5,
		BatchTimeout:   time.Second,
		BatchSize:      100,
		ExportTimeout:  30 * time.Second,
		ResourceAttrs: map[string]string{
			"deployment.environment": "test",
		},
	}
	
	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}
	
	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	
	if !manager.IsInitialized() {
		t.Error("Expected manager to be initialized")
	}
	
	if !manager.IsEnabled() {
		t.Error("Expected manager to be enabled")
	}
	
	tracer := manager.GetTracer()
	if tracer == nil {
		t.Error("Expected tracer to be available")
	}
	
	meter := manager.GetMeter()
	if meter == nil {
		t.Error("Expected meter to be available")
	}
	
	// Test shutdown
	err = manager.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown OTEL manager: %v", err)
	}
	
	if manager.IsInitialized() {
		t.Error("Expected manager to not be initialized after shutdown")
	}
}

func TestOTELManagerWithExporters(t *testing.T) {
	// Skip this test if we can't connect to a real OTLP endpoint
	// This test is mainly to verify the initialization logic works
	config := &OTELConfig{
		Enabled:         true,
		ServiceName:     "test-service",
		ServiceVersion:  "1.0.0",
		// Don't set endpoints to avoid connection errors in tests
		SamplingRate:    1.0,
		BatchTimeout:    time.Second,
		BatchSize:       100,
		ExportTimeout:   30 * time.Second,
		Insecure:        true,
		Headers: map[string]string{
			"x-api-key": "test-key",
		},
	}
	
	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}
	
	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	
	if !manager.IsInitialized() {
		t.Error("Expected manager to be initialized")
	}
	
	tracer := manager.GetTracer()
	if tracer == nil {
		t.Error("Expected tracer to be available")
	}
	
	meter := manager.GetMeter()
	if meter == nil {
		t.Error("Expected meter to be available")
	}
	
	// Test that we can create spans and metrics
	_, span := tracer.Start(ctx, "test-span")
	span.End()
	
	counter, err := meter.Int64Counter("test-counter")
	if err != nil {
		t.Errorf("Failed to create counter: %v", err)
	} else {
		counter.Add(ctx, 1)
	}
	
	// Test shutdown
	err = manager.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown OTEL manager: %v", err)
	}
}

func TestOTELManagerDoubleInitialization(t *testing.T) {
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "test-service"
	
	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}
	
	ctx := context.Background()
	
	// First initialization
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	
	// Second initialization should be a no-op
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Second initialization should not fail: %v", err)
	}
	
	if !manager.IsInitialized() {
		t.Error("Expected manager to remain initialized")
	}
	
	// Cleanup
	manager.Shutdown(ctx)
}

func TestOTELManagerGettersBeforeInitialization(t *testing.T) {
	config := DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "test-service"
	
	manager, err := NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}
	
	// Test getters before initialization - should return no-op implementations
	tracer := manager.GetTracer()
	if tracer == nil {
		t.Error("Expected tracer to be available even before initialization")
	}
	
	meter := manager.GetMeter()
	if meter == nil {
		t.Error("Expected meter to be available even before initialization")
	}
}
