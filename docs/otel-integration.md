# OpenTelemetry Integration Guide

This guide provides comprehensive examples and troubleshooting information for integrating the Treasure Data CLI with OpenTelemetry observability platforms.

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration Examples](#configuration-examples)
- [Platform Integrations](#platform-integrations)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Quick Start

Enable OpenTelemetry tracing for your CLI operations:

```bash
# Basic tracing with OTLP endpoint
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces

# Run CLI commands with tracing
tdcli queries list --database mydb
```

## Configuration Examples

### Environment Variables

The CLI supports all standard OpenTelemetry environment variables:

```bash
# Core OTEL configuration
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_SERVICE_VERSION=1.0.0

# Trace configuration
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_SAMPLING_RATE=0.1

# Metric configuration  
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:4318/v1/metrics

# Authentication headers
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer token123,x-api-key=key456"

# TLS configuration
export OTEL_EXPORTER_OTLP_INSECURE=false
```

### CLI Flags

Override environment variables with CLI flags:

```bash
tdcli queries list \
  --otel-enabled \
  --otel-service-name=my-tdcli \
  --otel-trace-endpoint=https://api.honeycomb.io/v1/traces \
  --otel-sampling-rate=0.05
```

## Platform Integrations

### Jaeger

#### Docker Compose Setup

```yaml
# docker-compose.yml
version: '3.8'
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"
      - "14250:14250"
      - "4317:4317"   # OTLP gRPC
      - "4318:4318"   # OTLP HTTP
    environment:
      - COLLECTOR_OTLP_ENABLED=true
```

#### Configuration

```bash
# Start Jaeger
docker-compose up -d jaeger

# Configure tdcli
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:4318/v1/metrics

# Run commands and view traces at http://localhost:16686
tdcli queries submit --database mydb --query "SELECT COUNT(*) FROM users"
```

### Honeycomb

#### Configuration

```bash
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://api.honeycomb.io/v1/traces
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=YOUR_API_KEY"
export OTEL_SAMPLING_RATE=0.1
```

#### Team Configuration

```bash
# For Honeycomb Classic
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=YOUR_API_KEY,x-honeycomb-dataset=tdcli"

# For Honeycomb Environments  
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=YOUR_API_KEY"
export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,service.version=1.0.0"
```

### Datadog

#### Configuration

```bash
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://trace.agent.datadoghq.com/v0.4/traces
export OTEL_EXPORTER_OTLP_HEADERS="dd-api-key=YOUR_API_KEY"
export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,env=production,version=1.0.0"
```

#### Agent Configuration

```bash
# Using Datadog Agent
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:4318/v1/metrics

# Ensure Datadog Agent has OTLP enabled in datadog.yaml:
# otlp_config:
#   receiver:
#     protocols:
#       grpc:
#         endpoint: 0.0.0.0:4317
#       http:
#         endpoint: 0.0.0.0:4318
```

### New Relic

#### Configuration

```bash
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://otlp.nr-data.net:4318/v1/traces
export OTEL_EXPORTER_OTLP_HEADERS="api-key=YOUR_LICENSE_KEY"
export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,service.version=1.0.0"
```

### Grafana Cloud

#### Configuration

```bash
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://tempo-prod-04-prod-us-central-0.grafana.net/otlp/v1/traces
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Basic $(echo -n "USER_ID:API_TOKEN" | base64)"
```

### OpenTelemetry Collector

#### Collector Configuration

```yaml
# otel-collector.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 1s
    send_batch_size: 1024
  
  resource:
    attributes:
      - key: deployment.environment
        value: production
        action: upsert

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true
  
  prometheus:
    endpoint: "0.0.0.0:8889"

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch, resource]
      exporters: [jaeger]
    
    metrics:
      receivers: [otlp]
      processors: [batch, resource]
      exporters: [prometheus]
```

#### Docker Compose with Collector

```yaml
version: '3.8'
services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otel-collector.yaml"]
    volumes:
      - ./otel-collector.yaml:/etc/otel-collector.yaml
    ports:
      - "4317:4317"   # OTLP gRPC
      - "4318:4318"   # OTLP HTTP
      - "8889:8889"   # Prometheus metrics
    depends_on:
      - jaeger

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"
      - "14250:14250"

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
```

#### CLI Configuration

```bash
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:4318/v1/metrics
```

## Troubleshooting

### Common Issues

#### 1. "OTEL not enabled" or No Traces Appearing

**Symptoms:**
- No traces appear in your observability platform
- CLI runs normally but no telemetry data

**Solutions:**
```bash
# Verify OTEL is enabled
echo $OTEL_ENABLED

# Check endpoint configuration
echo $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT

# Enable debug logging
export OTEL_LOG_LEVEL=debug
tdcli queries list --database mydb
```

#### 2. Connection Refused Errors

**Symptoms:**
```
Error: failed to export traces: connection refused
```

**Solutions:**
```bash
# Test endpoint connectivity
curl -v http://localhost:4318/v1/traces

# Check if collector/platform is running
docker ps | grep otel
docker ps | grep jaeger

# Verify firewall/network settings
telnet localhost 4318
```

#### 3. Authentication Failures

**Symptoms:**
```
Error: 401 Unauthorized
Error: 403 Forbidden
```

**Solutions:**
```bash
# Verify API key format
echo $OTEL_EXPORTER_OTLP_HEADERS

# Check platform-specific header requirements
# Honeycomb: x-honeycomb-team
# Datadog: dd-api-key  
# New Relic: api-key

# Test authentication separately
curl -H "x-honeycomb-team: YOUR_KEY" https://api.honeycomb.io/v1/traces
```

#### 4. High Memory Usage

**Symptoms:**
- CLI consuming excessive memory
- Out of memory errors

**Solutions:**
```bash
# Reduce sampling rate
export OTEL_SAMPLING_RATE=0.01

# Decrease batch size
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=512

# Reduce batch timeout
export OTEL_BSP_SCHEDULE_DELAY=1000
```

#### 5. Performance Degradation

**Symptoms:**
- CLI commands running slower than usual
- Timeouts on operations

**Solutions:**
```bash
# Disable synchronous exports
export OTEL_BSP_SCHEDULE_DELAY=5000

# Reduce sampling
export OTEL_SAMPLING_RATE=0.1

# Use async export
export OTEL_EXPORTER_OTLP_TIMEOUT=5000
```

### Debugging Commands

#### Check Configuration

```bash
# Display all OTEL environment variables
env | grep OTEL

# Test basic connectivity
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans":[]}' \
  $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
```

#### Validate Trace Export

```bash
# Run simple command with verbose output
OTEL_LOG_LEVEL=debug tdcli --help

# Check for export errors in logs
tdcli queries list 2>&1 | grep -i otel
```

#### Network Diagnostics

```bash
# Test OTLP HTTP endpoint
curl -v -X POST \
  -H "Content-Type: application/x-protobuf" \
  $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT

# Test OTLP gRPC endpoint (if using gRPC)
grpcurl -plaintext localhost:4317 list
```

### Platform-Specific Troubleshooting

#### Jaeger Issues

```bash
# Check Jaeger UI
open http://localhost:16686

# Verify OTLP receiver is enabled
docker logs jaeger-container-name | grep -i otlp

# Test direct Jaeger endpoint
curl http://localhost:14268/api/traces
```

#### Honeycomb Issues

```bash
# Validate API key
curl -H "x-honeycomb-team: YOUR_KEY" \
  https://api.honeycomb.io/1/auth

# Check dataset configuration
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=KEY,x-honeycomb-dataset=tdcli"
```

#### Datadog Issues

```bash
# Test Datadog Agent
curl http://localhost:8126/info

# Verify agent OTLP configuration
docker exec datadog-agent agent configcheck | grep -i otlp
```

## Best Practices

### Sampling Strategies

```bash
# Production: Low sampling rate
export OTEL_SAMPLING_RATE=0.01

# Development: High sampling rate  
export OTEL_SAMPLING_RATE=1.0

# Critical operations: Always sample
export OTEL_SAMPLING_RATE=1.0
```

### Resource Attributes

```bash
# Standard service attributes
export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,service.version=1.0.0,deployment.environment=production"

# Custom attributes
export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,team=data-engineering,region=us-east-1"
```

### Security Considerations

```bash
# Use environment variables for sensitive data
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer $API_TOKEN"

# Enable TLS for production
export OTEL_EXPORTER_OTLP_INSECURE=false

# Rotate API keys regularly
# Store keys in secure credential management systems
```

### Performance Optimization

```bash
# Batch configuration for high throughput
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=2048
export OTEL_BSP_SCHEDULE_DELAY=2000

# Memory limits
export OTEL_BSP_MAX_QUEUE_SIZE=4096

# Export timeout
export OTEL_EXPORTER_OTLP_TIMEOUT=10000
```

### Monitoring OTEL Health

```bash
# Monitor export success rates
# Check platform dashboards for data ingestion

# Set up alerts for:
# - Export failures
# - High latency
# - Memory usage
# - Missing traces
```

For additional support, consult the [OpenTelemetry documentation](https://opentelemetry.io/docs/) or your observability platform's integration guides.
