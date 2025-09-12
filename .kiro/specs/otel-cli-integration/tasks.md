# Implementation Plan

-
  1. [x] Set up OpenTelemetry dependencies and core infrastructure
  - Add OpenTelemetry Go packages to go.mod (otel, otel/trace, otel/metric,
    otel/exporters/otlp)
  - Create otel package directory structure for OTEL-specific code
  - Implement basic OTEL manager interface and configuration structures
  - _Requirements: 2.3, 5.3_

-
  2. [x] Implement OTEL configuration and initialization
  - [x] 2.1 Create OTEL configuration structures and validation
    - Define OTELConfig struct with all configuration options
    - Implement configuration validation functions with proper error handling
    - Create configuration loading from environment variables and CLI flags
    - _Requirements: 2.1, 2.2, 2.4_

  - [x] 2.2 Implement OTEL manager with provider initialization
    - Create OTELManager struct with tracer and meter provider setup
    - Implement provider initialization with proper resource configuration
    - Add graceful shutdown functionality for OTEL providers
    - _Requirements: 2.3, 5.3, 6.3_

  - [x] 2.3 Add OTEL exporter configuration and setup
    - Implement OTLP trace exporter with configurable endpoints
    - Implement OTLP metric exporter with batch processing
    - Add support for custom headers and authentication
    - _Requirements: 6.2, 6.3, 6.4_

-
  3. [x] Extend CLI structure with OTEL flags and integration
  - [x] 3.1 Add OTEL configuration flags to CLI struct
    - Extend CLI struct in cmd/tdcli/cli.go with OTEL-specific flags
    - Implement flag parsing and validation for OTEL options
    - Add environment variable support for all OTEL configuration
    - _Requirements: 2.1, 2.2_

  - [x] 3.2 Integrate OTEL manager into CLI initialization
    - Modify main.go to initialize OTEL manager based on configuration
    - Add OTEL manager to CLIContext for command access
    - Implement proper OTEL shutdown in CLI cleanup
    - _Requirements: 5.1, 5.2_

-
  4. [x] Implement Trino client instrumentation
  - [x] 4.1 Extend TDTrinoClient with OTEL support
    - Add meter field to TDTrinoClient struct (tracer already exists)
    - Extend TDTrinoClientConfig with OTEL configuration options
    - Update NewTDTrinoClientWithTracing to accept meter parameter
    - _Requirements: 3.1, 3.3_

  - [x] 4.2 Add span creation for Trino operations
    - Instrument Query, QueryRow, and Exec methods with span creation
    - Add proper span attributes for database operations (db.system,
      db.statement, etc.)
    - Implement error recording and span status setting
    - _Requirements: 3.1, 3.4, 7.1, 7.4_

  - [x] 4.3 Implement Trino metrics collection
    - Create metrics for query duration, query count, and connection status
    - Add metrics recording in all Trino client methods
    - Implement proper metric labeling with database and operation type
    - _Requirements: 4.2, 4.3_

-
  5. [x] Implement HTTP client instrumentation for TD API calls
  - [x] 5.1 Create instrumented HTTP transport wrapper
    - Implement otelHTTPTransport struct wrapping http.RoundTripper
    - Add span creation for HTTP requests with proper attributes
    - Implement request/response size and duration metrics
    - _Requirements: 3.2, 3.4, 7.1_

  - [x] 5.2 Integrate HTTP instrumentation into TD Client
    - Modify Client struct to include tracer and meter fields
    - Update NewClient to accept OTEL configuration and create instrumented
      transport
    - Add API-specific span attributes (td.api_version, td.endpoint)
    - _Requirements: 3.2, 4.3, 7.1_

  - [x] 5.3 Implement API call metrics and error tracking
    - Create metrics for API request duration, count, and error rates
    - Add proper metric labels for API endpoints and HTTP status codes
    - Implement error counter with categorization by error type
    - _Requirements: 4.3, 7.4_

-
  6. [x] Add CLI command-level instrumentation
  - [x] 6.1 Implement command span creation
    - Create spans for each CLI command execution with command name and
      arguments
    - Add CLI-specific span attributes (cli.command, cli.version, cli.region)
    - Implement proper span hierarchy for nested operations
    - _Requirements: 1.1, 1.2, 3.3, 7.1_

  - [x] 6.2 Add CLI metrics collection
    - Create metrics for command duration, execution count, and success/failure
      rates
    - Implement metrics recording in command execution flow
    - Add proper metric labels for command types and outcomes
    - _Requirements: 4.1, 4.3_

-
  7. [x] Implement data sanitization and security measures
  - [x] 7.1 Create data sanitization utilities
    - Implement SQL query sanitization to remove sensitive literals
    - Create URL sanitization to mask API keys and sensitive parameters
    - Add span attribute size limiting and content filtering
    - _Requirements: 7.1_

  - [x] 7.2 Implement secure credential handling
    - Ensure API keys are not included in span attributes or metrics
    - Add configuration validation for secure transport options
    - Implement proper error message sanitization
    - _Requirements: 7.1_

-
  8. [x] Add comprehensive error handling and graceful degradation
  - [x] 8.1 Implement OTEL error handling
    - Create OTELError type with severity levels and proper error wrapping
    - Add graceful degradation when OTEL dependencies are missing
    - Implement fallback to no-op providers on configuration errors
    - _Requirements: 5.3, 5.4, 6.4_

  - [x] 8.2 Add export failure handling and retry logic
    - Implement exponential backoff for failed exports
    - Add circuit breaker pattern for persistent export failures
    - Create fallback logging when exports consistently fail
    - _Requirements: 6.4_

-
  9. [x] Create comprehensive test suite
  - [x] 9.1 Implement unit tests for OTEL components
    - Create test utilities with mock OTEL providers
    - Write unit tests for OTELManager initialization and configuration
    - Add tests for span creation, attribute setting, and metric recording
    - _Requirements: 1.1, 1.2, 1.3_

  - [x] 9.2 Add integration tests for instrumented clients
    - Create integration tests for Trino client with OTEL enabled
    - Write tests for HTTP client instrumentation and API calls
    - Add end-to-end tests for CLI command tracing
    - _Requirements: 3.1, 3.2, 4.1_

  - [x] 9.3 Implement performance and degradation tests
    - Create benchmarks comparing performance with/without OTEL
    - Add tests for graceful degradation scenarios
    - Implement memory usage and latency impact tests
    - _Requirements: 5.2, 5.4_

- [x] 10.  Add configuration validation and documentation
  - [x] 10.1 Implement configuration validation
    - Add validation for OTEL endpoint URLs and sampling rates
    - Create helpful error messages for invalid configurations
    - Implement configuration precedence testing
    - _Requirements: 2.4_

  - [x] 10.2 Create usage examples and integration guides
    - Write example configurations for common OTEL collectors
    - Create integration examples with popular observability platforms
    - Add troubleshooting guide for common OTEL issues
    - _Requirements: 2.1, 2.2_

-  [x] 11. Final integration and testing
  - [x] 11.1 Integrate all components and test end-to-end functionality
    - Verify complete trace propagation through CLI operations
    - Test metric collection and export for all instrumented components
    - Validate configuration loading and provider initialization
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [x] 11.2 Performance optimization and cleanup
    - Optimize span creation and metric recording for minimal overhead
    - Implement proper resource cleanup and memory management
    - Add final performance validation and benchmarking
    - _Requirements: 5.2, 6.1, 6.3_
