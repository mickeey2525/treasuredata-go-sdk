# OpenTelemetry Troubleshooting Guide

This guide helps diagnose and resolve common issues when integrating tdcli with OpenTelemetry observability platforms.

## Quick Diagnostics

### Check OTEL Status

```bash
# Verify OTEL is enabled
echo "OTEL Enabled: $OTEL_ENABLED"
echo "Service Name: $OTEL_SERVICE_NAME"
echo "Trace Endpoint: $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
echo "Sampling Rate: $OTEL_SAMPLING_RATE"

# List all OTEL environment variables
env | grep OTEL | sort
```

### Test Basic Connectivity

```bash
# Test OTLP HTTP endpoint
curl -v -X POST \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans":[]}' \
  "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"

# Test with authentication headers
curl -v -X POST \
  -H "Content-Type: application/json" \
  -H "$(echo $OTEL_EXPORTER_OTLP_HEADERS | tr ',' '\n' | head -1)" \
  -d '{"resourceSpans":[]}' \
  "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
```

## Common Issues and Solutions

### 1. No Traces Appearing

#### Symptoms
- CLI runs normally but no traces appear in observability platform
- No error messages related to OTEL

#### Diagnostic Steps
```bash
# Check if OTEL is enabled
if [ "$OTEL_ENABLED" != "true" ]; then
    echo "❌ OTEL is not enabled"
    echo "Solution: export OTEL_ENABLED=true"
fi

# Check endpoint configuration
if [ -z "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" ]; then
    echo "❌ Trace endpoint not configured"
    echo "Solution: export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces"
fi

# Check sampling rate
if [ "$OTEL_SAMPLING_RATE" = "0" ] || [ "$OTEL_SAMPLING_RATE" = "0.0" ]; then
    echo "❌ Sampling rate is 0 - no traces will be generated"
    echo "Solution: export OTEL_SAMPLING_RATE=1.0"
fi
```

#### Solutions
```bash
# Enable OTEL with basic configuration
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_SAMPLING_RATE=1.0

# Test with a simple command
tdcli --help
```

### 2. Connection Refused Errors

#### Symptoms
```
Error: failed to export traces: connection refused
Error: dial tcp 127.0.0.1:4318: connect: connection refused
```

#### Diagnostic Steps
```bash
# Test endpoint connectivity
telnet localhost 4318
# or
nc -zv localhost 4318

# Check if collector/platform is running
docker ps | grep -E "(otel|jaeger|collector)"

# Test HTTP endpoint
curl -I http://localhost:4318/v1/traces
```

#### Solutions

**For Jaeger:**
```bash
# Start Jaeger with OTLP support
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -e COLLECTOR_OTLP_ENABLED=true \
  jaegertracing/all-in-one:latest
```

**For OTEL Collector:**
```bash
# Start OTEL Collector
docker run -d --name otel-collector \
  -p 4317:4317 \
  -p 4318:4318 \
  -v $(pwd)/otel-collector.yaml:/etc/otel-collector.yaml \
  otel/opentelemetry-collector-contrib:latest \
  --config=/etc/otel-collector.yaml
```

**For Cloud Platforms:**
```bash
# Verify endpoint URL format
# Correct: https://api.honeycomb.io/v1/traces
# Incorrect: https://api.honeycomb.io/traces (missing /v1)

# Test cloud endpoint
curl -I https://api.honeycomb.io/v1/traces
```

### 3. Authentication Failures

#### Symptoms
```
Error: 401 Unauthorized
Error: 403 Forbidden
Error: invalid API key
```

#### Diagnostic Steps
```bash
# Check authentication headers
echo "Headers: $OTEL_EXPORTER_OTLP_HEADERS"

# Validate header format (should be key1=value1,key2=value2)
echo "$OTEL_EXPORTER_OTLP_HEADERS" | grep -E '^[^=]+=.*(,[^=]+=.*)*$'
```

#### Platform-Specific Solutions

**Honeycomb:**
```bash
# Test API key
curl -H "x-honeycomb-team: YOUR_API_KEY" \
  https://api.honeycomb.io/1/auth

# Correct configuration
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=YOUR_API_KEY"

# For Honeycomb Classic (with dataset)
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=YOUR_API_KEY,x-honeycomb-dataset=tdcli"
```

**Datadog:**
```bash
# Test API key
curl -H "DD-API-KEY: YOUR_API_KEY" \
  https://api.datadoghq.com/api/v1/validate

# Correct configuration
export OTEL_EXPORTER_OTLP_HEADERS="dd-api-key=YOUR_API_KEY"
```

**New Relic:**
```bash
# Test license key
curl -H "api-key: YOUR_LICENSE_KEY" \
  https://api.newrelic.com/v2/applications.json

# Correct configuration
export OTEL_EXPORTER_OTLP_HEADERS="api-key=YOUR_LICENSE_KEY"
```

### 4. High Memory Usage

#### Symptoms
- CLI consuming excessive memory
- Out of memory errors
- System becoming unresponsive

#### Diagnostic Steps
```bash
# Monitor memory usage
ps aux | grep tdcli
top -p $(pgrep tdcli)

# Check OTEL configuration
echo "Batch Size: $OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
echo "Queue Size: $OTEL_BSP_MAX_QUEUE_SIZE"
echo "Sampling Rate: $OTEL_SAMPLING_RATE"
```

#### Solutions
```bash
# Reduce memory usage
export OTEL_SAMPLING_RATE=0.01  # 1% sampling
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=100
export OTEL_BSP_MAX_QUEUE_SIZE=1000
export OTEL_BSP_SCHEDULE_DELAY=1000  # Export more frequently

# Enable memory limiting
export OTEL_RESOURCE_DETECTOR_TIMEOUT=2s
export OTEL_EXPORTER_OTLP_TIMEOUT=5000
```

### 5. Performance Degradation

#### Symptoms
- CLI commands running slower than usual
- Timeouts on operations
- Increased latency

#### Diagnostic Steps
```bash
# Benchmark with/without OTEL
time tdcli queries list  # With OTEL enabled

export OTEL_ENABLED=false
time tdcli queries list  # With OTEL disabled

# Check export delays
echo "Schedule Delay: $OTEL_BSP_SCHEDULE_DELAY"
echo "Export Timeout: $OTEL_EXPORTER_OTLP_TIMEOUT"
```

#### Solutions
```bash
# Optimize for performance
export OTEL_SAMPLING_RATE=0.1  # Reduce sampling
export OTEL_BSP_SCHEDULE_DELAY=5000  # Less frequent exports
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=1000  # Larger batches
export OTEL_EXPORTER_OTLP_TIMEOUT=3000  # Shorter timeout

# Use async processing
export OTEL_BSP_EXPORT_TIMEOUT=2000
```

### 6. TLS/SSL Certificate Issues

#### Symptoms
```
Error: x509: certificate signed by unknown authority
Error: tls: handshake failure
```

#### Diagnostic Steps
```bash
# Test TLS connection
openssl s_client -connect api.honeycomb.io:443 -servername api.honeycomb.io

# Check certificate chain
curl -vI https://api.honeycomb.io/v1/traces
```

#### Solutions
```bash
# For development/testing (not recommended for production)
export OTEL_EXPORTER_OTLP_INSECURE=true

# For production with custom certificates
export OTEL_EXPORTER_OTLP_CERTIFICATE=/path/to/ca-cert.pem

# For self-signed certificates
export OTEL_EXPORTER_OTLP_INSECURE=true  # Only for testing
```

### 7. Span Attribute Issues

#### Symptoms
- Missing span attributes
- Sensitive data in traces
- Attribute size limits exceeded

#### Diagnostic Steps
```bash
# Enable debug logging to see span details
export OTEL_LOG_LEVEL=debug
tdcli queries list --database sample_datasets 2>&1 | grep -i span
```

#### Solutions
```bash
# Configure attribute limits
export OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT=1000
export OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT=100

# Configure resource attributes
export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,service.version=1.0.0,deployment.environment=production"

# The CLI automatically sanitizes sensitive data, but you can verify:
# - SQL queries have literals removed
# - API keys are masked in URLs
# - Authentication headers are excluded
```

### 8. Export Failures and Retries

#### Symptoms
```
Error: failed to export after 3 retries
Error: context deadline exceeded
```

#### Diagnostic Steps
```bash
# Check network connectivity
ping -c 3 api.honeycomb.io

# Test endpoint with timeout
timeout 10s curl -I https://api.honeycomb.io/v1/traces
```

#### Solutions
```bash
# Configure retry behavior
export OTEL_EXPORTER_OTLP_TIMEOUT=10000  # 10 seconds
export OTEL_BSP_SCHEDULE_DELAY=2000      # 2 seconds between batches

# For unreliable networks
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=100  # Smaller batches
export OTEL_EXPORTER_OTLP_TIMEOUT=15000    # Longer timeout
```

## Platform-Specific Troubleshooting

### Jaeger Issues

```bash
# Check Jaeger UI
curl -I http://localhost:16686

# Verify OTLP receiver is enabled
docker logs jaeger-container 2>&1 | grep -i otlp

# Test Jaeger API directly
curl -X POST http://localhost:14268/api/traces \
  -H "Content-Type: application/json" \
  -d '{"data":[{"traceID":"test","spans":[]}]}'
```

### Honeycomb Issues

```bash
# Validate API key and team
curl -H "x-honeycomb-team: $HONEYCOMB_API_KEY" \
  https://api.honeycomb.io/1/auth

# Check dataset (for Classic)
curl -H "x-honeycomb-team: $HONEYCOMB_API_KEY" \
  https://api.honeycomb.io/1/datasets

# Test trace ingestion
curl -X POST https://api.honeycomb.io/v1/traces \
  -H "x-honeycomb-team: $HONEYCOMB_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans":[]}'
```

### Datadog Issues

```bash
# Check Datadog Agent status
curl http://localhost:8126/info

# Verify OTLP configuration
docker exec datadog-agent agent configcheck | grep -i otlp

# Test Datadog API
curl -H "DD-API-KEY: $DD_API_KEY" \
  https://api.datadoghq.com/api/v1/validate

# Check APM service
curl -H "DD-API-KEY: $DD_API_KEY" \
  -H "DD-APPLICATION-KEY: $DD_APP_KEY" \
  "https://api.datadoghq.com/api/v1/apm/services"
```

### New Relic Issues

```bash
# Test license key
curl -H "api-key: $NEW_RELIC_LICENSE_KEY" \
  https://api.newrelic.com/v2/applications.json

# Check OTLP endpoint
curl -I https://otlp.nr-data.net:4318/v1/traces

# Verify region-specific endpoint
# US: https://otlp.nr-data.net:4318
# EU: https://otlp.eu01.nr-data.net:4318
```

## Debug Mode and Logging

### Enable Debug Logging

```bash
# Enable OTEL debug logging
export OTEL_LOG_LEVEL=debug

# Run command with verbose output
tdcli queries list --database sample_datasets 2>&1 | tee debug.log

# Filter for OTEL-related messages
grep -i otel debug.log
grep -i span debug.log
grep -i export debug.log
```

### Log Analysis

```bash
# Check for common error patterns
grep -E "(failed|error|timeout|refused)" debug.log

# Look for successful exports
grep -E "(exported|sent|success)" debug.log

# Check span creation
grep -E "(span.*start|span.*end)" debug.log
```

## Performance Monitoring

### Measure OTEL Overhead

```bash
#!/bin/bash
# performance-test.sh

echo "Testing CLI performance with/without OTEL..."

# Test without OTEL
export OTEL_ENABLED=false
echo "Without OTEL:"
time tdcli queries list --database sample_datasets

# Test with OTEL
export OTEL_ENABLED=true
export OTEL_SAMPLING_RATE=1.0
echo "With OTEL (100% sampling):"
time tdcli queries list --database sample_datasets

# Test with reduced sampling
export OTEL_SAMPLING_RATE=0.1
echo "With OTEL (10% sampling):"
time tdcli queries list --database sample_datasets
```

### Memory Usage Monitoring

```bash
#!/bin/bash
# memory-test.sh

# Monitor memory usage during CLI operations
(
    while true; do
        ps -o pid,vsz,rss,comm -p $(pgrep tdcli) 2>/dev/null || break
        sleep 1
    done
) &

# Run CLI command
tdcli queries submit --database mydb --query "SELECT COUNT(*) FROM large_table"

# Stop monitoring
kill %1
```

## Recovery Procedures

### Reset OTEL Configuration

```bash
# Clear all OTEL environment variables
unset $(env | grep OTEL | cut -d= -f1)

# Set minimal working configuration
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_SAMPLING_RATE=0.1
```

### Fallback to No-Op Mode

```bash
# Disable OTEL completely
export OTEL_ENABLED=false

# Or use minimal configuration that won't interfere
export OTEL_ENABLED=true
export OTEL_SAMPLING_RATE=0.0  # No sampling
```

### Emergency Debugging

```bash
# Create minimal test case
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli-debug
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_SAMPLING_RATE=1.0
export OTEL_LOG_LEVEL=debug

# Run simplest possible command
tdcli --help 2>&1 | tee emergency-debug.log

# Analyze output
grep -E "(error|failed|success|exported)" emergency-debug.log
```

## Getting Help

### Information to Collect

When reporting issues, please include:

1. **Environment Information:**
   ```bash
   echo "OS: $(uname -a)"
   echo "tdcli version: $(tdcli --version)"
   echo "Go version: $(go version)"
   ```

2. **OTEL Configuration:**
   ```bash
   env | grep OTEL | sort
   ```

3. **Error Messages:**
   ```bash
   tdcli your-command 2>&1 | tee error.log
   ```

4. **Network Connectivity:**
   ```bash
   curl -I $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
   ```

5. **Platform-Specific Info:**
   - Collector/Agent version and configuration
   - Observability platform account/region
   - Network topology (proxy, firewall, etc.)

### Support Resources

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [OTLP Specification](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md)
- Platform-specific documentation:
  - [Jaeger OTLP](https://www.jaegertracing.io/docs/1.50/apis/#opentelemetry-protocol-otlp)
  - [Honeycomb OTEL](https://docs.honeycomb.io/getting-data-in/opentelemetry/)
  - [Datadog OTEL](https://docs.datadoghq.com/tracing/setup_overview/open_standards/)
  - [New Relic OTEL](https://docs.newrelic.com/docs/more-integrations/open-source-telemetry-integrations/opentelemetry/)

This troubleshooting guide should help resolve most common issues with OpenTelemetry integration. For complex issues, consider enabling debug logging and analyzing the output systematically.
