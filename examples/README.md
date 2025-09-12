# OpenTelemetry Integration Examples

This directory contains comprehensive examples and configuration files for integrating the Treasure Data CLI (tdcli) with various OpenTelemetry observability platforms.

## Quick Start

1. **Choose your observability platform** from the configurations below
2. **Copy and customize** the relevant configuration script
3. **Source the configuration** to set environment variables
4. **Run tdcli commands** with automatic tracing enabled

## Available Configurations

### Local Development

| Platform | Configuration File | Description |
|----------|-------------------|-------------|
| **Jaeger** | [`otel-configs/jaeger-docker-compose.yml`](otel-configs/jaeger-docker-compose.yml) | Complete Docker setup with Jaeger all-in-one |
| **OTEL Collector** | [`otel-configs/otel-collector.yaml`](otel-configs/otel-collector.yaml) | Production-ready collector configuration |

### Cloud Platforms

| Platform | Configuration File | Description |
|----------|-------------------|-------------|
| **Honeycomb** | [`otel-configs/honeycomb-config.sh`](otel-configs/honeycomb-config.sh) | Honeycomb Cloud integration with validation |
| **Datadog** | [`otel-configs/datadog-config.sh`](otel-configs/datadog-config.sh) | Datadog APM with agent setup |
| **Grafana Cloud** | [`otel-configs/grafana-cloud-config.sh`](otel-configs/grafana-cloud-config.sh) | Grafana Cloud Tempo integration |

## Usage Examples

### Jaeger (Local Development)

```bash
# Start Jaeger
docker-compose -f otel-configs/jaeger-docker-compose.yml up -d

# Configure tdcli
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces

# Run commands with tracing
tdcli queries list --database sample_datasets

# View traces at http://localhost:16686
```

### Honeycomb

```bash
# Configure for Honeycomb
source otel-configs/honeycomb-config.sh

# Validate configuration
source otel-configs/honeycomb-config.sh validate

# Run commands with tracing
tdcli queries submit --database mydb --query "SELECT COUNT(*) FROM users"

# View traces in Honeycomb UI
```

### Datadog

```bash
# Configure for Datadog
source otel-configs/datadog-config.sh

# Set up Datadog Agent
source otel-configs/datadog-config.sh setup-agent

# Start agent with Docker
source otel-configs/datadog-config.sh docker-compose
docker-compose -f docker-compose.datadog.yml up -d

# Run commands with tracing
tdcli queries list --database mydb
```

### Grafana Cloud

```bash
# Configure for Grafana Cloud
source otel-configs/grafana-cloud-config.sh

# Validate configuration
source otel-configs/grafana-cloud-config.sh validate

# Run commands with tracing
tdcli bulk-import --database mydb --table mytable --file data.csv
```

## Configuration Options

### Environment Variables

All configurations support standard OpenTelemetry environment variables:

```bash
# Core configuration
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=tdcli
export OTEL_SERVICE_VERSION=1.0.0

# Endpoints
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:4318/v1/metrics

# Authentication
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer token123"

# Sampling
export OTEL_SAMPLING_RATE=0.1  # 10% sampling

# Performance tuning
export OTEL_BSP_SCHEDULE_DELAY=2000
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=512
export OTEL_EXPORTER_OTLP_TIMEOUT=10000
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

## Observability Data

### Traces

The CLI automatically creates spans for:

- **CLI Commands**: Each command execution with arguments and metadata
- **Trino Queries**: Query submission, execution, and result retrieval
- **API Calls**: HTTP requests to Treasure Data services
- **Bulk Operations**: Import/export operations with progress tracking

### Metrics

Automatic metrics collection includes:

- **Command Metrics**: Duration, success/failure rates, execution counts
- **Query Metrics**: Query duration, row counts, data transfer volumes
- **API Metrics**: Request duration, response codes, payload sizes
- **Error Metrics**: Error rates categorized by type and operation

### Span Attributes

Rich context is automatically added to spans:

```json
{
  "cli.command": "queries.submit",
  "cli.version": "1.0.0",
  "cli.region": "us-east-1",
  "db.system": "trino",
  "db.name": "sample_datasets",
  "db.statement": "SELECT COUNT(*) FROM users",
  "http.method": "POST",
  "http.status_code": 200,
  "td.api_version": "v4",
  "user.account_id": "12345"
}
```

## Security and Privacy

### Data Sanitization

The CLI automatically sanitizes sensitive data:

- **SQL Queries**: Literal values are removed or masked
- **API Keys**: Removed from URLs and span attributes
- **Authentication**: Headers excluded from traces
- **Personal Data**: User emails and names are excluded

### Example Sanitization

```sql
-- Original query
SELECT * FROM users WHERE email = 'user@example.com' AND age > 25

-- Sanitized in traces
SELECT * FROM users WHERE email = ? AND age > ?
```

## Performance Considerations

### Sampling Strategies

Choose appropriate sampling rates based on your environment:

```bash
# Development: High sampling for detailed debugging
export OTEL_SAMPLING_RATE=1.0

# Staging: Moderate sampling for testing
export OTEL_SAMPLING_RATE=0.1

# Production: Low sampling for cost control
export OTEL_SAMPLING_RATE=0.01
```

### Performance Impact

Typical performance overhead:

- **No Sampling**: ~1-2% CPU overhead
- **10% Sampling**: ~0.1-0.2% CPU overhead
- **1% Sampling**: ~0.01-0.02% CPU overhead

### Memory Usage

Configure batch sizes to control memory usage:

```bash
# Low memory environments
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=100
export OTEL_BSP_MAX_QUEUE_SIZE=1000

# High throughput environments
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=1000
export OTEL_BSP_MAX_QUEUE_SIZE=4000
```

## Troubleshooting

### Quick Diagnostics

```bash
# Check configuration
env | grep OTEL

# Test connectivity
curl -I $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT

# Enable debug logging
export OTEL_LOG_LEVEL=debug
tdcli queries list 2>&1 | grep -i otel
```

### Common Issues

1. **No traces appearing**: Check `OTEL_ENABLED=true` and endpoint configuration
2. **Connection refused**: Verify collector/platform is running and accessible
3. **Authentication errors**: Validate API keys and header format
4. **High memory usage**: Reduce sampling rate and batch sizes
5. **Performance issues**: Increase batch delays and reduce sampling

For detailed troubleshooting, see [`../docs/otel-troubleshooting.md`](../docs/otel-troubleshooting.md).

## Platform-Specific Guides

### Jaeger

- **UI**: http://localhost:16686
- **Best for**: Local development and testing
- **Setup time**: < 5 minutes with Docker

### Honeycomb

- **UI**: https://ui.honeycomb.io
- **Best for**: Production observability and debugging
- **Features**: Advanced querying, BubbleUp, SLOs

### Datadog

- **UI**: https://app.datadoghq.com/apm
- **Best for**: Full-stack monitoring with APM
- **Features**: Service maps, profiling, alerts

### Grafana Cloud

- **UI**: https://your-instance.grafana.net
- **Best for**: Open-source stack with managed hosting
- **Features**: Tempo, Prometheus, Loki integration

## Advanced Configurations

### Custom Resource Attributes

```bash
export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,service.version=1.0.0,deployment.environment=production,team=data-engineering,region=us-east-1"
```

### Multi-Platform Export

Use OTEL Collector to export to multiple platforms simultaneously:

```yaml
# otel-collector.yaml
exporters:
  jaeger:
    endpoint: jaeger:14250
  otlp/honeycomb:
    endpoint: https://api.honeycomb.io/v1/traces
    headers:
      x-honeycomb-team: YOUR_API_KEY
  otlp/datadog:
    endpoint: http://datadog-agent:4318/v1/traces

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger, otlp/honeycomb, otlp/datadog]
```

### Custom Sampling

```bash
# Always sample critical operations
export OTEL_RESOURCE_ATTRIBUTES="$OTEL_RESOURCE_ATTRIBUTES,sampling.priority=1"

# Never sample health checks
export OTEL_RESOURCE_ATTRIBUTES="$OTEL_RESOURCE_ATTRIBUTES,sampling.priority=0"
```

## Best Practices

### Production Deployment

1. **Use OTEL Collector** for better reliability and performance
2. **Configure appropriate sampling** to control costs
3. **Set up monitoring** for the telemetry pipeline itself
4. **Implement alerting** on export failures
5. **Regular credential rotation** for security

### Development Workflow

1. **Start with local Jaeger** for immediate feedback
2. **Use 100% sampling** during development
3. **Test with production sampling rates** before deployment
4. **Validate sanitization** of sensitive data

### Cost Optimization

1. **Monitor trace volume** and adjust sampling accordingly
2. **Use head-based sampling** for predictable costs
3. **Configure retention policies** appropriately
4. **Archive old traces** to cheaper storage

## Support and Resources

- **Main Documentation**: [`../docs/otel-integration.md`](../docs/otel-integration.md)
- **Troubleshooting Guide**: [`../docs/otel-troubleshooting.md`](../docs/otel-troubleshooting.md)
- **OpenTelemetry Docs**: https://opentelemetry.io/docs/
- **Platform Documentation**:
  - [Jaeger](https://www.jaegertracing.io/docs/)
  - [Honeycomb](https://docs.honeycomb.io/)
  - [Datadog](https://docs.datadoghq.com/tracing/)
  - [Grafana](https://grafana.com/docs/tempo/)

## Contributing

To add support for additional platforms:

1. Create a new configuration script in `otel-configs/`
2. Follow the existing naming pattern: `platform-config.sh`
3. Include validation, examples, and troubleshooting functions
4. Update this README with the new platform
5. Test with the actual platform before submitting

## License

These configuration examples are provided under the same license as the main tdcli project.
