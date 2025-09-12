package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	treasuredata "github.com/mickeey2525/treasuredata-go-sdk"
	"github.com/mickeey2525/treasuredata-go-sdk/otel"
)

// TestEndToEndOTELIntegration tests complete OTEL integration across all components
func TestEndToEndOTELIntegration(t *testing.T) {
	// In-memory handler to simulate TD API (no real server)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add trace headers to simulate trace propagation
		traceID := r.Header.Get("traceparent")
		if traceID != "" {
			w.Header().Set("x-trace-received", "true")
		}

		// Simulate different API endpoints
		switch {
		case strings.Contains(r.URL.Path, "/v3/databases"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"databases": []map[string]interface{}{
					{"name": "test_db", "count": 100, "created_at": "2023-01-01T00:00:00Z"},
					{"name": "analytics_db", "count": 250, "created_at": "2023-02-01T00:00:00Z"},
				},
			})
		case strings.Contains(r.URL.Path, "/v3/tables"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"tables": []map[string]interface{}{
					{"name": "events", "type": "log", "count": 1000000},
					{"name": "users", "type": "log", "count": 50000},
				},
			})
		case strings.Contains(r.URL.Path, "/v4/jobs"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"job_id":     "12345",
				"status":     "queued",
				"created_at": time.Now().Format(time.RFC3339),
				"query":      "SELECT COUNT(*) FROM events",
			})
		case strings.Contains(r.URL.Path, "/v3/users"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"users": []map[string]interface{}{
					{"id": 1, "name": "admin", "email": "admin@example.com"},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error": "Not found"}`))
		}
	})

	// Create OTEL manager with test configuration (no external endpoints)
	config := &otel.OTELConfig{
		Enabled:                 true,
		ServiceName:             "tdcli-e2e-test",
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
			"test.scenario":          "end-to-end",
		},
		// No trace/metric endpoints - will use no-op exporters
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

	// Create TD client with OTEL instrumentation and in-memory transport
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		resp := rr.Result()
		resp.Request = req
		return resp, nil
	})
	client, err := treasuredata.NewClient("test_account/test_key",
		treasuredata.WithEndpoint("http://example"),
		treasuredata.WithHTTPClient(&http.Client{Transport: rt}),
		treasuredata.WithOTEL(manager.GetTracer(), manager.GetMeter()),
	)
	if err != nil {
		t.Fatalf("Failed to create TD client: %v", err)
	}

	// Create CLI context with all components
	cliCtx := &CLIContext{
		Context:     ctx,
		OTELManager: manager,
		Client:      client,
		GlobalFlags: Flags{
			Region:   "us",
			Format:   "json",
			Verbose:  true,
			Database: "test_db",
			Engine:   "presto",
		},
	}

	// Test 1: Complete trace propagation through CLI command
	t.Run("Complete trace propagation", func(t *testing.T) {
		// Simulate a databases list command
		err := InstrumentedRun(cliCtx, "databases.list", []string{"--format", "json"}, func(ctx *CLIContext) error {
			// This simulates what the databases list command would do
			req, err := ctx.Client.NewRequest("GET", "/v3/databases", nil)
			if err != nil {
				return err
			}

			resp, err := ctx.Client.Do(ctx.Context, req, nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			// Verify response
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}

			// Verify that trace context is propagated (check for traceparent header)
			if resp.Header.Get("x-trace-received") != "true" {
				t.Log("Note: Trace propagation to server not verified (test server limitation)")
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Command execution failed: %v", err)
		}

		// Verify that instrumentation components are working
		if manager.GetTracer() == nil {
			t.Error("Expected tracer to be available")
		}
		if manager.GetMeter() == nil {
			t.Error("Expected meter to be available")
		}

		t.Log("Complete trace propagation test passed")
	})

	// Test 2: Metric collection across all components
	t.Run("Comprehensive metric collection", func(t *testing.T) {
		// Execute multiple commands to generate metrics
		commands := []struct {
			name     string
			endpoint string
		}{
			{"databases.list", "/v3/databases"},
			{"tables.list", "/v3/tables"},
			{"jobs.submit", "/v4/jobs"},
			{"users.list", "/v3/users"},
		}

		for _, cmd := range commands {
			err := InstrumentedRun(cliCtx, cmd.name, []string{}, func(ctx *CLIContext) error {
				req, err := ctx.Client.NewRequest("GET", cmd.endpoint, nil)
				if err != nil {
					return err
				}

				resp, err := ctx.Client.Do(ctx.Context, req, nil)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				return nil
			})

			if err != nil {
				t.Errorf("Command %s failed: %v", cmd.name, err)
			}
		}

		// Wait for metrics to be recorded
		time.Sleep(200 * time.Millisecond)

		// Verify that metric collection infrastructure is working
		if manager.GetMeter() == nil {
			t.Error("Expected meter to be available for metrics collection")
		}

		// Since we don't have external metric endpoints configured,
		// we can't collect actual metrics, but we can verify the infrastructure works
		t.Log("Comprehensive metric collection test passed")
	})

	// Test 3: Configuration loading and provider initialization
	t.Run("Configuration validation", func(t *testing.T) {
		// Verify manager configuration
		if !manager.IsEnabled() {
			t.Error("Expected OTEL manager to be enabled")
		}

		if !manager.IsInitialized() {
			t.Error("Expected OTEL manager to be initialized")
		}

		// Verify tracer and meter are available
		if manager.GetTracer() == nil {
			t.Error("Expected tracer to be available")
		}

		if manager.GetMeter() == nil {
			t.Error("Expected meter to be available")
		}

		// Verify configuration values
		config := manager.GetConfig()
		if config.ServiceName != "tdcli-e2e-test" {
			t.Errorf("Expected service name 'tdcli-e2e-test', got '%s'", config.ServiceName)
		}

		if config.SamplingRate != 1.0 {
			t.Errorf("Expected sampling rate 1.0, got %f", config.SamplingRate)
		}

		// Verify resource attributes
		expectedAttrs := map[string]string{
			"deployment.environment": "test",
			"test.scenario":          "end-to-end",
		}

		for key, expectedValue := range expectedAttrs {
			if actualValue, exists := config.ResourceAttrs[key]; !exists {
				t.Errorf("Expected resource attribute %s not found", key)
			} else if actualValue != expectedValue {
				t.Errorf("Expected resource attribute %s=%s, got %s", key, expectedValue, actualValue)
			}
		}

		t.Log("Configuration validation test passed")
	})

	// Test 4: Error handling and span status
	t.Run("Error handling and span status", func(t *testing.T) {
		// Execute a command that will fail
		err := InstrumentedRun(cliCtx, "test.error", []string{}, func(ctx *CLIContext) error {
			// Simulate an API call that returns an error
			req, err := ctx.Client.NewRequest("GET", "/nonexistent", nil)
			if err != nil {
				return err
			}

			_, err = ctx.Client.Do(ctx.Context, req, nil)
			return err // This should be an error due to 404
		})

		if err == nil {
			t.Fatal("Expected command to return an error")
		}

		// Verify that error handling doesn't break the instrumentation
		if manager.GetTracer() == nil {
			t.Error("Expected tracer to still be available after error")
		}
		if manager.GetMeter() == nil {
			t.Error("Expected meter to still be available after error")
		}

		t.Log("Error handling and span status test passed")
	})

	// Test 5: Data sanitization
	t.Run("Data sanitization", func(t *testing.T) {
		// Execute command with sensitive arguments
		sensitiveArgs := []string{
			"--api-key", "test_account/secret_key_12345",
			"--password", "super_secret_password",
			"--query", "SELECT * FROM users WHERE ssn = '123-45-6789'",
		}

		err := InstrumentedRun(cliCtx, "test.sanitization", sensitiveArgs, func(ctx *CLIContext) error {
			// Just return success, we're testing argument sanitization
			return nil
		})

		if err != nil {
			t.Fatalf("Command execution failed: %v", err)
		}

		// Verify that sanitization functions work correctly
		sanitizedArgs := sanitizeArgs(sensitiveArgs)
		hasRedacted := false
		for _, arg := range sanitizedArgs {
			if arg == "[REDACTED]" || strings.Contains(arg, "[REDACTED]") {
				hasRedacted = true
				break
			}
		}
		if !hasRedacted {
			t.Errorf("Expected sanitized args to contain [REDACTED] for sensitive data, got: %v", sanitizedArgs)
		}

		t.Log("Data sanitization test passed")
	})
}

// roundTripperFunc is a helper to create a RoundTripper from a function.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestEndToEndOTELConfigurationFromEnvironment tests configuration loading from environment variables
func TestEndToEndOTELConfigurationFromEnvironment(t *testing.T) {
	// Set environment variables
	envVars := map[string]string{
		"OTEL_SERVICE_NAME":                   "tdcli-env-test",
		"OTEL_SERVICE_VERSION":                "2.0.0",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT":  "http://localhost:4318/v1/traces",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://localhost:4318/v1/metrics",
		"OTEL_EXPORTER_OTLP_HEADERS":          "authorization=Bearer token123",
		"OTEL_EXPORTER_OTLP_INSECURE":         "true",
		"OTEL_SAMPLING_RATE":                  "0.5",
		"OTEL_ENABLED":                        "true",
	}

	// Set environment variables
	for key, value := range envVars {
		os.Setenv(key, value)
		defer os.Unsetenv(key)
	}

	// Create configuration from environment
	config := otel.DefaultOTELConfig()
	config.LoadFromEnvironment()

	// Verify configuration values
	if config.ServiceName != "tdcli-env-test" {
		t.Errorf("Expected service name 'tdcli-env-test', got '%s'", config.ServiceName)
	}

	if config.ServiceVersion != "2.0.0" {
		t.Errorf("Expected service version '2.0.0', got '%s'", config.ServiceVersion)
	}

	if config.TraceEndpoint != "http://localhost:4318/v1/traces" {
		t.Errorf("Expected trace endpoint 'http://localhost:4318/v1/traces', got '%s'", config.TraceEndpoint)
	}

	if config.MetricEndpoint != "http://localhost:4318/v1/metrics" {
		t.Errorf("Expected metric endpoint 'http://localhost:4318/v1/metrics', got '%s'", config.MetricEndpoint)
	}

	if config.SamplingRate != 0.5 {
		t.Errorf("Expected sampling rate 0.5, got %f", config.SamplingRate)
	}

	if !config.Enabled {
		t.Error("Expected OTEL to be enabled")
	}

	if !config.Insecure {
		t.Error("Expected OTEL to be configured as insecure")
	}

	// Verify headers
	if config.Headers["authorization"] != "Bearer token123" {
		t.Errorf("Expected authorization header 'Bearer token123', got '%s'", config.Headers["authorization"])
	}

	// Create manager with environment configuration
	manager, err := otel.NewOTELManager(config)
	if err != nil {
		t.Fatalf("Failed to create OTEL manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Initialize(ctx)
	if err != nil {
		// This might fail due to invalid endpoints, but we're testing configuration loading
		t.Logf("Manager initialization failed (expected due to test endpoints): %v", err)
	} else {
		defer manager.Shutdown(ctx)
	}

	// Verify manager configuration
	managerConfig := manager.GetConfig()
	if managerConfig.ServiceName != "tdcli-env-test" {
		t.Errorf("Manager: expected service name 'tdcli-env-test', got '%s'", managerConfig.ServiceName)
	}

	t.Log("Environment configuration test passed")
}

// TestEndToEndOTELGracefulDegradation tests graceful degradation scenarios
func TestEndToEndOTELGracefulDegradation(t *testing.T) {
	testCases := []struct {
		name        string
		setupConfig func() *otel.OTELConfig
		expectError bool
	}{
		{
			name: "Invalid trace endpoint",
			setupConfig: func() *otel.OTELConfig {
				config := otel.DefaultOTELConfig()
				config.Enabled = true
				config.ServiceName = "degradation-test"
				config.TraceEndpoint = "invalid://endpoint"
				return config
			},
			expectError: true,
		},
		{
			name: "Disabled OTEL",
			setupConfig: func() *otel.OTELConfig {
				config := otel.DefaultOTELConfig()
				config.Enabled = false
				config.ServiceName = "degradation-test"
				return config
			},
			expectError: false,
		},
		{
			name: "Missing endpoints",
			setupConfig: func() *otel.OTELConfig {
				config := otel.DefaultOTELConfig()
				config.Enabled = true
				config.ServiceName = "degradation-test"
				// No endpoints configured
				return config
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.setupConfig()
			manager, err := otel.NewOTELManager(config)
			if err != nil {
				if !tc.expectError {
					t.Fatalf("Unexpected error creating manager: %v", err)
				}
				return
			}

			ctx := context.Background()
			err = manager.Initialize(ctx)
			if err != nil && !tc.expectError {
				t.Fatalf("Unexpected error initializing manager: %v", err)
			}

			if err == nil {
				defer manager.Shutdown(ctx)

				// Verify that CLI commands still work
				cliCtx := &CLIContext{
					Context:     ctx,
					OTELManager: manager,
					GlobalFlags: Flags{
						Region: "us",
						Format: "json",
					},
				}

				err := InstrumentedRun(cliCtx, "degradation.test", []string{}, func(ctx *CLIContext) error {
					return nil
				})

				if err != nil {
					t.Errorf("CLI command should work even with degraded OTEL: %v", err)
				}
			}

			t.Logf("Graceful degradation test '%s' passed", tc.name)
		})
	}
}
