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
				Enabled:      true,
				ServiceName:  "test-service",
				SamplingRate: 0.5,
				BatchTimeout: 1 * time.Second,
				BatchSize:    100,
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
