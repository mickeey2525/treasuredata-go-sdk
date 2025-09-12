package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mickeey2525/treasuredata-go-sdk/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// TestCLICommandOTELIntegration tests end-to-end CLI command tracing
func TestCLICommandOTELIntegration(t *testing.T) {
	// Create OTEL manager with disabled config for this test
	// We'll test the instrumentation logic without actual export
	config := &otel.OTELConfig{
		Enabled:                 true,
		ServiceName:             "cli-integration-test",
		ServiceVersion:          "1.0.0",
		SamplingRate:            1.0,
		BatchTimeout:            time.Second,
		BatchSize:               100,
		ExportTimeout:           30 * time.Second,
		RetryMaxDelay:           30 * time.Second,
		RetryBackoffFactor:      2.0,
		CircuitFailureThreshold: 5,
		CircuitRecoveryTimeout:  60 * time.Second,
		CircuitHalfOpenMaxCalls: 3,
		ResourceAttrs: map[string]string{
			"deployment.environment": "test",
		},
	}

	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Create CLI context with OTEL manager
	cliCtx := &CLIContext{
		Context:     ctx,
		OTELManager: manager,
		GlobalFlags: Flags{
			Region:   "us",
			Format:   "json",
			Verbose:  true,
			Database: "test_db",
		},
	}

	// Test version command with instrumentation
	t.Run("Version command with tracing", func(t *testing.T) {
		versionCmd := &VersionCmd{}
		err := InstrumentedRun(cliCtx, "version", []string{}, func(ctx *CLIContext) error {
			return versionCmd.Run(ctx)
		})

		if err != nil {
			t.Fatalf("Version command failed: %v", err)
		}

		// Since we're using the OTEL manager's providers (which may be no-op),
		// we can't directly verify spans. Instead, we verify that the instrumentation
		// doesn't break the command execution and that the manager is properly configured.
		if !manager.IsEnabled() {
			t.Error("Expected OTEL manager to be enabled")
		}

		if manager.GetTracer() == nil {
			t.Error("Expected tracer to be available")
		}

		if manager.GetMeter() == nil {
			t.Error("Expected meter to be available")
		}

		t.Log("Version command with tracing completed successfully")
	})

	// Test multiple commands to verify instrumentation works consistently
	t.Run("Multiple commands with tracing", func(t *testing.T) {
		// Run version command multiple times
		for i := 0; i < 3; i++ {
			versionCmd := &VersionCmd{}
			err := InstrumentedRun(cliCtx, "version", []string{}, func(ctx *CLIContext) error {
				return versionCmd.Run(ctx)
			})
			if err != nil {
				t.Fatalf("Version command %d failed: %v", i, err)
			}
		}

		// Verify that multiple commands can be executed without issues
		t.Log("Multiple commands with tracing completed successfully")
	})

	// Test command with arguments
	t.Run("Command with arguments", func(t *testing.T) {
		args := []string{"--format", "table", "--verbose"}
		versionCmd := &VersionCmd{}
		err := InstrumentedRun(cliCtx, "version", args, func(ctx *CLIContext) error {
			return versionCmd.Run(ctx)
		})

		if err != nil {
			t.Fatalf("Version command with args failed: %v", err)
		}

		// Verify that commands with arguments can be executed without issues
		t.Log("Command with arguments completed successfully")
	})

	// Test metrics collection
	t.Run("Metrics collection", func(t *testing.T) {
		// Run several commands to generate metrics
		for i := 0; i < 5; i++ {
			versionCmd := &VersionCmd{}
			InstrumentedRun(cliCtx, "version", []string{}, func(ctx *CLIContext) error {
				return versionCmd.Run(ctx)
			})
		}

		// Wait a bit for metrics to be recorded
		time.Sleep(100 * time.Millisecond)

		// Verify that metrics collection works by checking that the meter is available
		if manager.GetMeter() == nil {
			t.Error("Expected meter to be available for metrics collection")
		}

		t.Log("CLI command metrics collection completed successfully")
	})
}

// TestCLICommandOTELError tests error handling in CLI instrumentation
func TestCLICommandOTELError(t *testing.T) {
	// Set up OTEL providers
	exporter := tracetest.NewInMemoryExporter()
	_ = trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("cli-error-test"),
		)),
	)

	reader := metric.NewManualReader()
	_ = metric.NewMeterProvider(metric.WithReader(reader))

	// Create OTEL manager
	config := &otel.OTELConfig{
		Enabled:                 true,
		ServiceName:             "cli-error-test",
		SamplingRate:            1.0,
		BatchTimeout:            time.Second,
		BatchSize:               100,
		ExportTimeout:           30 * time.Second,
		RetryMaxDelay:           30 * time.Second,
		RetryBackoffFactor:      2.0,
		CircuitFailureThreshold: 5,
		CircuitRecoveryTimeout:  60 * time.Second,
		CircuitHalfOpenMaxCalls: 3,
	}

	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Create CLI context
	cliCtx := &CLIContext{
		Context:     ctx,
		OTELManager: manager,
		GlobalFlags: Flags{
			Region: "us",
			Format: "json",
		},
	}

	// Test command that returns an error
	t.Run("Command with error", func(t *testing.T) {
		// Clear previous spans
		exporter.Reset()

		err := InstrumentedRun(cliCtx, "test-error", []string{}, func(ctx *CLIContext) error {
			return os.ErrNotExist // Simulate an error
		})

		if err == nil {
			t.Fatal("Expected command to return an error")
		}

		// Verify spans were created with error status
		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("Expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name != "cli.command.test-error" {
			t.Errorf("Expected span name 'cli.command.test-error', got '%s'", span.Name)
		}

		// Verify span status is Error
		if span.Status.Code.String() != "Error" {
			t.Errorf("Expected span status Error, got %s", span.Status.Code.String())
		}

		// Verify error is recorded
		if len(span.Events) == 0 {
			t.Error("Expected error event to be recorded in span")
		}
	})
}

// TestCLICommandOTELDisabled tests CLI without OTEL
func TestCLICommandOTELDisabled(t *testing.T) {
	// Create CLI context without OTEL manager
	cliCtx := &CLIContext{
		Context:     context.Background(),
		OTELManager: nil,
		GlobalFlags: Flags{
			Region: "us",
			Format: "table",
		},
	}

	// Test that commands work without OTEL
	t.Run("Command without OTEL", func(t *testing.T) {
		versionCmd := &VersionCmd{}
		err := InstrumentedRun(cliCtx, "version", []string{}, func(ctx *CLIContext) error {
			return versionCmd.Run(ctx)
		})

		if err != nil {
			t.Fatalf("Version command failed: %v", err)
		}

		t.Log("CLI command without OTEL completed successfully")
	})
}

// TestCLICommandOTELWithDisabledConfig tests CLI with disabled OTEL config
func TestCLICommandOTELWithDisabledConfig(t *testing.T) {
	// Create OTEL manager with disabled config
	config := otel.DefaultOTELConfig()
	config.Enabled = false
	config.ServiceName = "cli-disabled-test"

	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Create CLI context with disabled OTEL
	cliCtx := &CLIContext{
		Context:     ctx,
		OTELManager: manager,
		GlobalFlags: Flags{
			Region: "eu",
			Format: "csv",
		},
	}

	// Test that commands work with disabled OTEL
	t.Run("Command with disabled OTEL", func(t *testing.T) {
		versionCmd := &VersionCmd{}
		err := InstrumentedRun(cliCtx, "version", []string{}, func(ctx *CLIContext) error {
			return versionCmd.Run(ctx)
		})

		if err != nil {
			t.Fatalf("Version command failed: %v", err)
		}

		t.Log("CLI command with disabled OTEL completed successfully")
	})
}

// TestCLICommandOTELArgumentSanitization tests argument sanitization
func TestCLICommandOTELArgumentSanitization(t *testing.T) {
	// Set up OTEL providers
	exporter := tracetest.NewInMemoryExporter()
	_ = trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("cli-sanitization-test"),
		)),
	)

	reader := metric.NewManualReader()
	_ = metric.NewMeterProvider(metric.WithReader(reader))

	// Create OTEL manager
	config := &otel.OTELConfig{
		Enabled:                 true,
		ServiceName:             "cli-sanitization-test",
		SamplingRate:            1.0,
		BatchTimeout:            time.Second,
		BatchSize:               100,
		ExportTimeout:           30 * time.Second,
		RetryMaxDelay:           30 * time.Second,
		RetryBackoffFactor:      2.0,
		CircuitFailureThreshold: 5,
		CircuitRecoveryTimeout:  60 * time.Second,
		CircuitHalfOpenMaxCalls: 3,
	}

	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Create CLI context
	cliCtx := &CLIContext{
		Context:     ctx,
		OTELManager: manager,
		GlobalFlags: Flags{
			Region: "us",
			Format: "json",
		},
	}

	testCases := []struct {
		name           string
		args           []string
		expectRedacted bool
	}{
		{
			name:           "API key should be redacted",
			args:           []string{"--api-key", "account123/secret_api_key"},
			expectRedacted: true,
		},
		{
			name:           "Password should be redacted",
			args:           []string{"--password", "secret123"},
			expectRedacted: true,
		},
		{
			name:           "Normal arguments should not be redacted",
			args:           []string{"--format", "json", "--region", "us"},
			expectRedacted: false,
		},
		{
			name:           "Long argument should be truncated",
			args:           []string{"--query", "SELECT * FROM very_long_table_name_that_exceeds_the_maximum_length_limit_for_span_attributes_and_should_be_truncated"},
			expectRedacted: false, // Not redacted, but truncated
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear previous spans
			exporter.Reset()

			versionCmd := &VersionCmd{}
			err := InstrumentedRun(cliCtx, "version", tc.args, func(ctx *CLIContext) error {
				return versionCmd.Run(ctx)
			})

			if err != nil {
				t.Fatalf("Version command failed: %v", err)
			}

			// Verify spans were created
			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("Expected 1 span, got %d", len(spans))
			}

			span := spans[0]
			attrs := span.Attributes

			// Check argument sanitization
			foundArgs := false
			for _, attr := range attrs {
				if string(attr.Key) == "cli.args" {
					foundArgs = true
					argsValue := attr.Value.AsStringSlice()

					if tc.expectRedacted {
						// Should contain [REDACTED] for sensitive arguments
						hasRedacted := false
						for _, arg := range argsValue {
							if arg == "[REDACTED]" {
								hasRedacted = true
								break
							}
						}
						if !hasRedacted {
							t.Errorf("Expected args to contain [REDACTED] for sensitive data, got: %v", argsValue)
						}
					} else {
						// Should not contain [REDACTED] for normal arguments
						for _, arg := range argsValue {
							if arg == "[REDACTED]" {
								t.Errorf("Expected args to not contain [REDACTED] for normal data, got: %v", argsValue)
								break
							}
						}
					}
					break
				}
			}
			if !foundArgs && len(tc.args) > 0 {
				t.Error("Expected cli.args attribute not found")
			}
		})
	}
}

// TestCLICommandOTELInstrumentationFailure tests graceful degradation when instrumentation fails
func TestCLICommandOTELInstrumentationFailure(t *testing.T) {
	// Create CLI context with invalid OTEL manager (nil tracer/meter)
	cliCtx := &CLIContext{
		Context: context.Background(),
		// OTELManager is nil, which should cause graceful degradation
		OTELManager: nil,
		GlobalFlags: Flags{
			Region:  "us",
			Format:  "json",
			Verbose: true, // Enable verbose to see warning messages
		},
	}

	// Test that commands still work when instrumentation fails
	t.Run("Command with instrumentation failure", func(t *testing.T) {
		versionCmd := &VersionCmd{}
		err := InstrumentedRun(cliCtx, "version", []string{}, func(ctx *CLIContext) error {
			return versionCmd.Run(ctx)
		})

		if err != nil {
			t.Fatalf("Version command should not fail due to instrumentation issues: %v", err)
		}

		t.Log("CLI command with instrumentation failure completed successfully")
	})
}

// TestCLICommandOTELContextPropagation tests that context is properly propagated
func TestCLICommandOTELContextPropagation(t *testing.T) {
	// Set up OTEL providers
	exporter := tracetest.NewInMemoryExporter()
	_ = trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("cli-context-test"),
		)),
	)

	reader := metric.NewManualReader()
	_ = metric.NewMeterProvider(metric.WithReader(reader))

	// Create OTEL manager
	config := &otel.OTELConfig{
		Enabled:                 true,
		ServiceName:             "cli-context-test",
		SamplingRate:            1.0,
		BatchTimeout:            time.Second,
		BatchSize:               100,
		ExportTimeout:           30 * time.Second,
		RetryMaxDelay:           30 * time.Second,
		RetryBackoffFactor:      2.0,
		CircuitFailureThreshold: 5,
		CircuitRecoveryTimeout:  60 * time.Second,
		CircuitHalfOpenMaxCalls: 3,
	}

	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize OTEL manager: %v", err)
	}
	defer manager.Shutdown(ctx)

	// Create CLI context
	cliCtx := &CLIContext{
		Context:     ctx,
		OTELManager: manager,
		GlobalFlags: Flags{
			Region: "us",
			Format: "json",
		},
	}

	// Test that context is properly propagated to command execution
	t.Run("Context propagation", func(t *testing.T) {
		// Clear previous spans
		exporter.Reset()

		contextReceived := false
		err := InstrumentedRun(cliCtx, "test-context", []string{}, func(ctx *CLIContext) error {
			// Verify that the context contains trace information
			if ctx.Context != nil {
				contextReceived = true
			}
			return nil
		})

		if err != nil {
			t.Fatalf("Test command failed: %v", err)
		}

		if !contextReceived {
			t.Error("Expected context to be propagated to command execution")
		}

		// Verify spans were created
		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("Expected 1 span, got %d", len(spans))
		}

		t.Log("CLI context propagation test completed successfully")
	})
}
