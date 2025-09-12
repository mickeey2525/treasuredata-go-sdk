// Package otel provides OpenTelemetry integration for the Treasure Data CLI.
//
// This package implements comprehensive observability capabilities including:
// - Distributed tracing for CLI operations, Trino queries, and API calls
// - Metrics collection for performance monitoring
// - Configurable sampling and export settings
// - Graceful degradation when OTEL is disabled or unavailable
//
// The main entry point is the OTELManager which handles provider initialization
// and configuration management. The package follows OpenTelemetry best practices
// and provides minimal performance overhead when disabled.
//
// Example usage:
//
//	config := &otel.OTELConfig{
//		Enabled:     true,
//		ServiceName: "tdcli",
//		TraceEndpoint: "http://localhost:4318/v1/traces",
//	}
//
//	manager, err := otel.NewOTELManager(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if err := manager.Initialize(context.Background()); err != nil {
//		log.Fatal(err)
//	}
//	defer manager.Shutdown(context.Background())
//
//	tracer := manager.GetTracer()
//	meter := manager.GetMeter()
package otel
