package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// CLIInstrumentation provides OpenTelemetry instrumentation for CLI commands
type CLIInstrumentation struct {
	tracer oteltrace.Tracer
	meter  metric.Meter

	// Metrics
	commandDuration metric.Float64Histogram
	commandCounter  metric.Int64Counter
	commandErrors   metric.Int64Counter
	commandSuccess  metric.Int64Counter
}

// NewCLIInstrumentation creates a new CLI instrumentation instance
func NewCLIInstrumentation(tracer oteltrace.Tracer, meter metric.Meter) (*CLIInstrumentation, error) {
	if tracer == nil || meter == nil {
		return nil, fmt.Errorf("tracer and meter cannot be nil")
	}

	// Create metrics instruments
	commandDuration, err := meter.Float64Histogram(
		"cli_command_duration",
		metric.WithDescription("Duration of CLI command execution in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create command duration histogram: %w", err)
	}

	commandCounter, err := meter.Int64Counter(
		"cli_command_total",
		metric.WithDescription("Total number of CLI commands executed"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create command counter: %w", err)
	}

	commandErrors, err := meter.Int64Counter(
		"cli_command_errors_total",
		metric.WithDescription("Total number of CLI command errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create command error counter: %w", err)
	}

	commandSuccess, err := meter.Int64Counter(
		"cli_command_success_total",
		metric.WithDescription("Total number of successful CLI commands"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create command success counter: %w", err)
	}

	return &CLIInstrumentation{
		tracer:          tracer,
		meter:           meter,
		commandDuration: commandDuration,
		commandCounter:  commandCounter,
		commandErrors:   commandErrors,
		commandSuccess:  commandSuccess,
	}, nil
}

// InstrumentCommand wraps command execution with OpenTelemetry instrumentation
func (i *CLIInstrumentation) InstrumentCommand(ctx context.Context, commandName string, args []string, flags Flags, fn func(context.Context) error) error {
	// Start timing
	startTime := time.Now()

	// Create span for command execution
	spanName := fmt.Sprintf("cli.command.%s", commandName)
	ctx, span := i.tracer.Start(ctx, spanName)
	defer span.End()

	// Add span attributes
	i.addSpanAttributes(span, commandName, args, flags)

	// Create metric attributes
	metricAttrs := i.createMetricAttributes(commandName, flags)

	// Increment command counter
	i.commandCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))

	// Execute the command
	err := fn(ctx)

	// Record execution duration
	duration := time.Since(startTime).Seconds()
	i.commandDuration.Record(ctx, duration, metric.WithAttributes(metricAttrs...))

	// Handle result and record metrics
	if err != nil {
		// Record error
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		// Add error attributes
		errorAttrs := append(metricAttrs,
			attribute.String("error.type", getErrorType(err)),
			attribute.String("error.message", sanitizeErrorMessage(err.Error())),
		)
		i.commandErrors.Add(ctx, 1, metric.WithAttributes(errorAttrs...))
	} else {
		// Record success
		span.SetStatus(codes.Ok, "Command completed successfully")
		i.commandSuccess.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
	}

	return err
}

// addSpanAttributes adds CLI-specific attributes to the span
func (i *CLIInstrumentation) addSpanAttributes(span oteltrace.Span, commandName string, args []string, flags Flags) {
	// Basic command information
	span.SetAttributes(
		attribute.String("cli.command", commandName),
		attribute.String("cli.version", version),
		attribute.String("cli.region", flags.Region),
		attribute.String("cli.format", flags.Format),
		attribute.Bool("cli.verbose", flags.Verbose),
	)

	// Add sanitized arguments (remove sensitive data)
	sanitizedArgs := sanitizeArgs(args)
	if len(sanitizedArgs) > 0 {
		span.SetAttributes(attribute.StringSlice("cli.args", sanitizedArgs))
	}

	// Add system information
	span.SetAttributes(
		attribute.String("os.type", runtime.GOOS),
		attribute.String("os.arch", runtime.GOARCH),
		attribute.String("runtime.version", runtime.Version()),
	)

	// Add database context if available
	if flags.Database != "" {
		span.SetAttributes(attribute.String("cli.database", flags.Database))
	}

	// Add engine context if available
	if flags.Engine != "" {
		span.SetAttributes(attribute.String("cli.engine", flags.Engine))
	}

	// Add SSL configuration context
	if flags.InsecureSkipVerify {
		span.SetAttributes(attribute.Bool("cli.ssl.insecure_skip_verify", true))
	}
	if flags.CertFile != "" {
		span.SetAttributes(attribute.Bool("cli.ssl.client_cert_configured", true))
	}
}

// createMetricAttributes creates attributes for metrics
func (i *CLIInstrumentation) createMetricAttributes(commandName string, flags Flags) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("command", commandName),
		attribute.String("region", flags.Region),
		attribute.String("format", flags.Format),
	}

	// Add database if available
	if flags.Database != "" {
		attrs = append(attrs, attribute.String("database", flags.Database))
	}

	// Add engine if available
	if flags.Engine != "" {
		attrs = append(attrs, attribute.String("engine", flags.Engine))
	}

	return attrs
}

// sanitizeArgs removes sensitive information from command arguments
func sanitizeArgs(args []string) []string {
	if len(args) == 0 {
		return args
	}

	sanitized := make([]string, len(args))
	for i, arg := range args {
		sanitized[i] = sanitizeArg(arg)
	}
	return sanitized
}

// sanitizeArg sanitizes a single argument
func sanitizeArg(arg string) string {
	// Remove potential API keys (format: account_id/api_key)
	if strings.Contains(arg, "/") && len(arg) > 10 {
		parts := strings.Split(arg, "/")
		if len(parts) == 2 && len(parts[1]) > 10 {
			return fmt.Sprintf("%s/[REDACTED]", parts[0])
		}
	}

	// Remove potential passwords or tokens
	lower := strings.ToLower(arg)
	if strings.Contains(lower, "password") ||
		strings.Contains(lower, "token") ||
		strings.Contains(lower, "secret") ||
		strings.Contains(lower, "key=") {
		return "[REDACTED]"
	}

	// Limit argument length to prevent excessive span sizes
	if len(arg) > 100 {
		return arg[:97] + "..."
	}

	return arg
}

// sanitizeErrorMessage removes sensitive information from error messages
func sanitizeErrorMessage(message string) string {
	// Remove API keys from error messages
	if strings.Contains(message, "/") {
		// Replace potential API key patterns
		parts := strings.Split(message, " ")
		for i, part := range parts {
			if strings.Contains(part, "/") && len(part) > 10 {
				subparts := strings.Split(part, "/")
				if len(subparts) == 2 && len(subparts[1]) > 10 {
					parts[i] = fmt.Sprintf("%s/[REDACTED]", subparts[0])
				}
			}
		}
		message = strings.Join(parts, " ")
	}

	// Limit error message length
	if len(message) > 200 {
		return message[:197] + "..."
	}

	return message
}

// getErrorType extracts error type from error
func getErrorType(err error) string {
	if err == nil {
		return "none"
	}

	// Get the type name
	errorType := fmt.Sprintf("%T", err)

	// Remove package path, keep only the type name
	if lastDot := strings.LastIndex(errorType, "."); lastDot != -1 {
		errorType = errorType[lastDot+1:]
	}

	// Remove pointer indicator
	errorType = strings.TrimPrefix(errorType, "*")

	return errorType
}

// InstrumentedRun wraps the Kong Run method with instrumentation
func InstrumentedRun(ctx *CLIContext, commandName string, args []string, fn func(*CLIContext) error) error {
	// Check if OTEL is available and enabled
	if ctx.OTELManager == nil || !ctx.OTELManager.IsEnabled() {
		// Execute without instrumentation
		return fn(ctx)
	}

	// Create instrumentation
	instrumentation, err := NewCLIInstrumentation(
		ctx.OTELManager.GetTracer(),
		ctx.OTELManager.GetMeter(),
	)
	if err != nil {
		// Log warning and continue without instrumentation
		if ctx.GlobalFlags.Verbose {
			fmt.Fprintf(os.Stderr, "Warning: Failed to create CLI instrumentation: %v\n", err)
		}
		return fn(ctx)
	}

	// Instrument the command execution
	return instrumentation.InstrumentCommand(
		ctx.Context,
		commandName,
		args,
		ctx.GlobalFlags,
		func(instrumentedCtx context.Context) error {
			// Update context and execute
			originalCtx := ctx.Context
			ctx.Context = instrumentedCtx
			defer func() { ctx.Context = originalCtx }()

			return fn(ctx)
		},
	)
}
