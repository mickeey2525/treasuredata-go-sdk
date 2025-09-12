package main

import (
	"context"
	"testing"

	"github.com/mickeey2525/treasuredata-go-sdk/otel"
)

// TestCLIInstrumentationIntegration tests the full CLI instrumentation integration
func TestCLIInstrumentationIntegration(t *testing.T) {
	// Create OTEL configuration
	config := otel.DefaultOTELConfig()
	config.Enabled = true
	config.ServiceName = "tdcli-integration-test"

	// Create and initialize OTEL manager
	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	err = manager.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(context.Background())

	// Create CLI context
	ctx := &CLIContext{
		Context:     context.Background(),
		OTELManager: manager,
		GlobalFlags: Flags{
			Region:   "us",
			Format:   "json",
			Verbose:  true,
			Database: "test_db",
		},
	}

	// Test version command with instrumentation
	versionCmd := &VersionCmd{}
	err = versionCmd.Run(ctx)
	if err != nil {
		t.Fatalf("Version command failed: %v", err)
	}

	// Test another version command to verify multiple instrumented calls work
	err = versionCmd.Run(ctx)
	if err != nil {
		t.Fatalf("Second version command failed: %v", err)
	}

	t.Log("CLI instrumentation integration test completed successfully")
}

// TestCLIInstrumentationDisabled tests that CLI works when OTEL is disabled
func TestCLIInstrumentationDisabled(t *testing.T) {
	// Create CLI context without OTEL manager
	ctx := &CLIContext{
		Context:     context.Background(),
		OTELManager: nil,
		GlobalFlags: Flags{
			Region: "us",
			Format: "table",
		},
	}

	// Test version command without instrumentation
	versionCmd := &VersionCmd{}
	err := versionCmd.Run(ctx)
	if err != nil {
		t.Fatalf("Version command failed: %v", err)
	}

	t.Log("CLI without instrumentation test completed successfully")
}

// TestCLIInstrumentationWithDisabledOTEL tests CLI with disabled OTEL config
func TestCLIInstrumentationWithDisabledOTEL(t *testing.T) {
	// Create OTEL configuration with disabled flag
	config := otel.DefaultOTELConfig()
	config.Enabled = false
	config.ServiceName = "tdcli-disabled-test"

	// Create and initialize OTEL manager
	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	err = manager.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(context.Background())

	// Create CLI context
	ctx := &CLIContext{
		Context:     context.Background(),
		OTELManager: manager,
		GlobalFlags: Flags{
			Region: "eu",
			Format: "csv",
		},
	}

	// Test version command with disabled OTEL
	versionCmd := &VersionCmd{}
	err = versionCmd.Run(ctx)
	if err != nil {
		t.Fatalf("Version command failed: %v", err)
	}

	t.Log("CLI with disabled OTEL test completed successfully")
}
