package main

import (
	"context"
	"errors"
	"testing"

	"github.com/mickeey2525/treasuredata-go-sdk/otel"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func TestNewCLIInstrumentation(t *testing.T) {
	// Create test providers
	tracer := otelapi.Tracer("test")
	meter := otelapi.Meter("test")

	// Test successful creation
	instr, err := NewCLIInstrumentation(tracer, meter)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if instr == nil {
		t.Fatal("Expected instrumentation instance, got nil")
	}

	// Test with nil tracer
	_, err = NewCLIInstrumentation(nil, meter)
	if err == nil {
		t.Fatal("Expected error with nil tracer")
	}

	// Test with nil meter
	_, err = NewCLIInstrumentation(tracer, nil)
	if err == nil {
		t.Fatal("Expected error with nil meter")
	}
}

func TestInstrumentCommand(t *testing.T) {
	// Set up test providers
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(spanRecorder),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("test-cli"),
		)),
	)

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("test-cli"),
		)),
	)

	tracer := tracerProvider.Tracer("test")
	meter := meterProvider.Meter("test")

	instr, err := NewCLIInstrumentation(tracer, meter)
	if err != nil {
		t.Fatalf("Failed to create instrumentation: %v", err)
	}

	// Test successful command execution
	t.Run("successful command", func(t *testing.T) {
		ctx := context.Background()
		commandName := "test.command"
		args := []string{"arg1", "arg2"}
		flags := Flags{
			Region:   "us",
			Format:   "json",
			Verbose:  true,
			Database: "test_db",
		}

		executed := false
		err := instr.InstrumentCommand(ctx, commandName, args, flags, func(ctx context.Context) error {
			executed = true
			return nil
		})

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if !executed {
			t.Fatal("Command function was not executed")
		}

		// Check spans
		spans := spanRecorder.Ended()
		if len(spans) != 1 {
			t.Fatalf("Expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name() != "cli.command.test.command" {
			t.Errorf("Expected span name 'cli.command.test.command', got '%s'", span.Name())
		}

		// Check span attributes
		attrs := span.Attributes()
		expectedAttrs := map[string]interface{}{
			"cli.command":  commandName,
			"cli.region":   "us",
			"cli.format":   "json",
			"cli.verbose":  true,
			"cli.database": "test_db",
		}

		for key, expectedValue := range expectedAttrs {
			found := false
			for _, attr := range attrs {
				if string(attr.Key) == key {
					found = true
					switch v := expectedValue.(type) {
					case string:
						if attr.Value.AsString() != v {
							t.Errorf("Expected %s=%s, got %s", key, v, attr.Value.AsString())
						}
					case bool:
						if attr.Value.AsBool() != v {
							t.Errorf("Expected %s=%t, got %t", key, v, attr.Value.AsBool())
						}
					}
					break
				}
			}
			if !found {
				t.Errorf("Expected attribute %s not found", key)
			}
		}

		// Check metrics
		var rm metricdata.ResourceMetrics
		err = reader.Collect(ctx, &rm)
		if err != nil {
			t.Fatalf("Failed to collect metrics: %v", err)
		}

		// Verify we have metrics
		if len(rm.ScopeMetrics) == 0 {
			t.Fatal("Expected metrics, got none")
		}
	})

	// Test command with error
	t.Run("command with error", func(t *testing.T) {
		spanRecorder.Reset()
		ctx := context.Background()
		commandName := "test.error"
		args := []string{}
		flags := Flags{Region: "us"}

		testError := errors.New("test error")
		err := instr.InstrumentCommand(ctx, commandName, args, flags, func(ctx context.Context) error {
			return testError
		})

		if err != testError {
			t.Fatalf("Expected test error, got: %v", err)
		}

		// Check spans
		spans := spanRecorder.Ended()
		if len(spans) != 1 {
			t.Fatalf("Expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Status().Code != codes.Error {
			t.Errorf("Expected error status, got: %v", span.Status().Code)
		}
	})
}

func TestSanitizeArgs(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "empty args",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "normal args",
			input:    []string{"database", "table", "query"},
			expected: []string{"database", "table", "query"},
		},
		{
			name:     "api key in args",
			input:    []string{"12345/abcdefghijklmnop", "database"},
			expected: []string{"12345/[REDACTED]", "database"},
		},
		{
			name:     "password in args",
			input:    []string{"--password=secret123", "database"},
			expected: []string{"[REDACTED]", "database"},
		},
		{
			name:     "long argument",
			input:    []string{string(make([]byte, 150))},
			expected: []string{string(make([]byte, 97)) + "..."},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeArgs(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("Expected %d args, got %d", len(tt.expected), len(result))
			}
			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("Expected arg[%d]=%s, got %s", i, expected, result[i])
				}
			}
		})
	}
}

func TestSanitizeErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "normal error",
			input:    "connection failed",
			expected: "connection failed",
		},
		{
			name:     "error with api key",
			input:    "authentication failed for 12345/abcdefghijklmnop",
			expected: "authentication failed for 12345/[REDACTED]",
		},
		{
			name:     "long error message",
			input:    string(make([]byte, 250)),
			expected: string(make([]byte, 197)) + "...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeErrorMessage(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestGetErrorType(t *testing.T) {
	tests := []struct {
		name     string
		input    error
		expected string
	}{
		{
			name:     "nil error",
			input:    nil,
			expected: "none",
		},
		{
			name:     "standard error",
			input:    errors.New("test"),
			expected: "errorString",
		},
		{
			name:     "custom error type",
			input:    &otel.OTELError{},
			expected: "OTELError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getErrorType(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestInstrumentedRun(t *testing.T) {
	// Test without OTEL manager
	t.Run("without OTEL manager", func(t *testing.T) {
		ctx := &CLIContext{
			Context:     context.Background(),
			OTELManager: nil,
			GlobalFlags: Flags{},
		}

		executed := false
		err := InstrumentedRun(ctx, "test", []string{}, func(ctx *CLIContext) error {
			executed = true
			return nil
		})

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if !executed {
			t.Fatal("Function was not executed")
		}
	})

	// Test with disabled OTEL
	t.Run("with disabled OTEL", func(t *testing.T) {
		config := &otel.OTELConfig{Enabled: false}
		manager, _ := otel.NewOTELManager(config)

		ctx := &CLIContext{
			Context:     context.Background(),
			OTELManager: manager,
			GlobalFlags: Flags{},
		}

		executed := false
		err := InstrumentedRun(ctx, "test", []string{}, func(ctx *CLIContext) error {
			executed = true
			return nil
		})

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if !executed {
			t.Fatal("Function was not executed")
		}
	})

	// Test with enabled OTEL
	t.Run("with enabled OTEL", func(t *testing.T) {
		config := otel.DefaultOTELConfig()
		config.Enabled = true
		config.ServiceName = "test-cli"

		manager, err := otel.NewOTELManager(config)
		if err != nil {
			t.Fatalf("Failed to create OTEL manager: %v", err)
		}

		// Initialize without endpoints (will use no-op providers)
		err = manager.Initialize(context.Background())
		if err != nil {
			t.Fatalf("Failed to initialize OTEL manager: %v", err)
		}

		ctx := &CLIContext{
			Context:     context.Background(),
			OTELManager: manager,
			GlobalFlags: Flags{Region: "us"},
		}

		executed := false
		err = InstrumentedRun(ctx, "test", []string{}, func(ctx *CLIContext) error {
			executed = true
			return nil
		})

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if !executed {
			t.Fatal("Function was not executed")
		}
	})
}
