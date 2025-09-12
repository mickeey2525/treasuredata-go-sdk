#!/bin/bash

# Grafana Cloud OpenTelemetry Configuration for tdcli
# This script sets up environment variables for Grafana Cloud integration

# =============================================================================
# Grafana Cloud Configuration
# =============================================================================

# Replace with your actual Grafana Cloud credentials
GRAFANA_CLOUD_USER_ID="your_user_id_here"
GRAFANA_CLOUD_API_TOKEN="your_api_token_here"

# Grafana Cloud instance details
GRAFANA_CLOUD_INSTANCE="your-instance"  # e.g., "mycompany"
GRAFANA_CLOUD_REGION="prod-us-central-0"  # Check your Grafana Cloud settings

# Environment configuration
ENVIRONMENT="production"
SERVICE_VERSION="1.0.0"

# =============================================================================
# Basic OTEL Configuration
# =============================================================================

export OTEL_ENABLED=true
export OTEL_SERVICE_NAME="tdcli"
export OTEL_SERVICE_VERSION="$SERVICE_VERSION"

# =============================================================================
# Grafana Cloud Endpoints
# =============================================================================

# Tempo (traces) endpoint
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="https://tempo-${GRAFANA_CLOUD_REGION}.grafana.net/otlp/v1/traces"

# Prometheus (metrics) endpoint - Note: Use Prometheus remote write for metrics
# OTEL metrics to Grafana Cloud require different setup
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="https://prometheus-${GRAFANA_CLOUD_REGION}.grafana.net/api/prom/push"

# =============================================================================
# Authentication
# =============================================================================

# Create base64 encoded credentials for Basic auth
GRAFANA_AUTH=$(echo -n "${GRAFANA_CLOUD_USER_ID}:${GRAFANA_CLOUD_API_TOKEN}" | base64)

export OTEL_EXPORTER_OTLP_HEADERS="authorization=Basic ${GRAFANA_AUTH}"

# =============================================================================
# Resource Attributes
# =============================================================================

export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,service.version=${SERVICE_VERSION},deployment.environment=${ENVIRONMENT},service.namespace=treasure-data,grafana.instance=${GRAFANA_CLOUD_INSTANCE}"

# =============================================================================
# Sampling Configuration
# =============================================================================

# Start with moderate sampling for cost control
export OTEL_SAMPLING_RATE=0.1  # 10% sampling

# =============================================================================
# Performance Tuning
# =============================================================================

# Optimize for Grafana Cloud
export OTEL_BSP_SCHEDULE_DELAY=2000   # 2 seconds
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=200
export OTEL_BSP_MAX_QUEUE_SIZE=1000

# Export timeout
export OTEL_EXPORTER_OTLP_TIMEOUT=10000  # 10 seconds

# =============================================================================
# Grafana Agent Configuration (Alternative)
# =============================================================================

create_grafana_agent_config() {
    cat > grafana-agent.yaml << EOF
server:
  log_level: info
  http_listen_port: 12345

traces:
  configs:
    - name: default
      receivers:
        otlp:
          protocols:
            grpc:
              endpoint: 0.0.0.0:4317
            http:
              endpoint: 0.0.0.0:4318
      
      remote_write:
        - endpoint: https://tempo-${GRAFANA_CLOUD_REGION}.grafana.net:443/tempo
          basic_auth:
            username: ${GRAFANA_CLOUD_USER_ID}
            password: ${GRAFANA_CLOUD_API_TOKEN}
      
      batch:
        timeout: 1s
        send_batch_size: 100

metrics:
  global:
    scrape_interval: 15s
    remote_write:
      - url: https://prometheus-${GRAFANA_CLOUD_REGION}.grafana.net/api/prom/push
        basic_auth:
          username: ${GRAFANA_CLOUD_USER_ID}
          password: ${GRAFANA_CLOUD_API_TOKEN}

  configs:
    - name: default
      scrape_configs:
        - job_name: 'grafana-agent'
          static_configs:
            - targets: ['localhost:12345']

logs:
  configs:
    - name: default
      clients:
        - url: https://logs-${GRAFANA_CLOUD_REGION}.grafana.net/loki/api/v1/push
          basic_auth:
            username: ${GRAFANA_CLOUD_USER_ID}
            password: ${GRAFANA_CLOUD_API_TOKEN}
      positions:
        filename: /tmp/positions.yaml
      scrape_configs:
        - job_name: tdcli-logs
          static_configs:
            - targets:
                - localhost
              labels:
                job: tdcli
                __path__: /var/log/tdcli/*.log
EOF

    echo "✅ Grafana Agent configuration created: grafana-agent.yaml"
    echo "   Start with: grafana-agent -config.file=grafana-agent.yaml"
}

# =============================================================================
# Docker Compose for Grafana Agent
# =============================================================================

create_docker_compose() {
    cat > docker-compose.grafana.yml << EOF
version: '3.8'

services:
  grafana-agent:
    image: grafana/agent:latest
    container_name: grafana-agent
    ports:
      - "4317:4317"  # OTLP gRPC
      - "4318:4318"  # OTLP HTTP
      - "12345:12345"  # Agent HTTP
    volumes:
      - ./grafana-agent.yaml:/etc/agent/agent.yaml
      - /var/log:/var/log:ro
    command:
      - -config.file=/etc/agent/agent.yaml
      - -server.http.address=0.0.0.0:12345
    environment:
      - AGENT_MODE=flow
    networks:
      - grafana-network

networks:
  grafana-network:
    driver: bridge
EOF

    echo "✅ Docker Compose configuration created: docker-compose.grafana.yml"
    echo "   Start with: docker-compose -f docker-compose.grafana.yml up -d"
}

# =============================================================================
# Validation and Testing
# =============================================================================

validate_config() {
    echo "Validating Grafana Cloud configuration..."
    
    # Check if credentials are set
    if [ "$GRAFANA_CLOUD_USER_ID" = "your_user_id_here" ] || [ "$GRAFANA_CLOUD_API_TOKEN" = "your_api_token_here" ]; then
        echo "❌ Please set your actual Grafana Cloud credentials"
        return 1
    fi
    
    # Test Tempo endpoint
    echo "Testing Tempo endpoint..."
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Basic ${GRAFANA_AUTH}" \
        "https://tempo-${GRAFANA_CLOUD_REGION}.grafana.net/api/echo")
    
    if [ "$response" = "200" ]; then
        echo "✅ Successfully connected to Grafana Cloud Tempo"
    else
        echo "⚠️  Could not verify Tempo connection (HTTP $response)"
        echo "   This might be normal - Tempo doesn't always have an echo endpoint"
    fi
    
    # Test Prometheus endpoint
    echo "Testing Prometheus endpoint..."
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Basic ${GRAFANA_AUTH}" \
        "https://prometheus-${GRAFANA_CLOUD_REGION}.grafana.net/api/v1/query?query=up")
    
    if [ "$response" = "200" ]; then
        echo "✅ Successfully connected to Grafana Cloud Prometheus"
    else
        echo "❌ Failed to connect to Grafana Cloud Prometheus (HTTP $response)"
        echo "Please check your credentials and region"
        return 1
    fi
    
    echo "✅ Configuration validated successfully"
    return 0
}

# =============================================================================
# Usage Examples
# =============================================================================

show_examples() {
    echo ""
    echo "=== Usage Examples ==="
    echo ""
    echo "1. Basic query with tracing:"
    echo "   tdcli queries list --database sample_datasets"
    echo ""
    echo "2. Submit query with custom attributes:"
    echo "   OTEL_RESOURCE_ATTRIBUTES=\"\$OTEL_RESOURCE_ATTRIBUTES,user.id=john.doe,team=data-engineering\" \\"
    echo "   tdcli queries submit --database mydb --query 'SELECT COUNT(*) FROM users'"
    echo ""
    echo "3. High-volume operation with sampling:"
    echo "   OTEL_SAMPLING_RATE=0.01 \\"
    echo "   tdcli bulk-import --database mydb --table mytable --file large-data.csv"
    echo ""
    echo "4. View traces in Grafana:"
    echo "   https://${GRAFANA_CLOUD_INSTANCE}.grafana.net/explore"
    echo "   - Select Tempo as data source"
    echo "   - Search for service.name=\"tdcli\""
    echo ""
    echo "5. Create dashboard queries:"
    echo "   - Trace volume: rate(traces_total[5m])"
    echo "   - Error rate: rate(traces_total{status=\"error\"}[5m])"
    echo "   - P95 latency: histogram_quantile(0.95, traces_duration_bucket)"
    echo ""
}

# =============================================================================
# Troubleshooting
# =============================================================================

troubleshoot() {
    echo ""
    echo "=== Troubleshooting ==="
    echo ""
    echo "1. Check environment variables:"
    echo "   env | grep OTEL"
    echo "   echo \"Auth: \$GRAFANA_AUTH\""
    echo ""
    echo "2. Test credentials:"
    echo "   curl -H \"Authorization: Basic \$GRAFANA_AUTH\" \\"
    echo "        \"https://prometheus-${GRAFANA_CLOUD_REGION}.grafana.net/api/v1/query?query=up\""
    echo ""
    echo "3. Verify region and instance:"
    echo "   - Check your Grafana Cloud dashboard for correct region"
    echo "   - Verify instance name in the URL"
    echo ""
    echo "4. Enable debug logging:"
    echo "   OTEL_LOG_LEVEL=debug tdcli queries list"
    echo ""
    echo "5. Check Grafana Cloud limits:"
    echo "   - Traces per minute limit"
    echo "   - Data retention period"
    echo "   - Active series limit"
    echo ""
    echo "6. Common issues:"
    echo "   - Wrong region: Check Grafana Cloud settings"
    echo "   - Invalid credentials: Regenerate API token"
    echo "   - Rate limiting: Reduce sampling rate"
    echo "   - Network issues: Check firewall/proxy settings"
    echo ""
    echo "7. Grafana Cloud URLs by region:"
    echo "   - US Central: prod-us-central-0"
    echo "   - US East: prod-us-east-0"
    echo "   - EU West: prod-eu-west-0"
    echo "   - Check your instance for the exact region"
    echo ""
}

# =============================================================================
# Main Script Logic
# =============================================================================

case "${1:-}" in
    "validate")
        validate_config
        ;;
    "examples")
        show_examples
        ;;
    "troubleshoot")
        troubleshoot
        ;;
    "agent-config")
        create_grafana_agent_config
        ;;
    "docker-compose")
        create_docker_compose
        ;;
    "test")
        validate_config && {
            echo ""
            echo "Running test command with tracing..."
            tdcli --help
            echo ""
            echo "Check Grafana Cloud for traces:"
            echo "https://${GRAFANA_CLOUD_INSTANCE}.grafana.net/explore"
        }
        ;;
    *)
        echo "Grafana Cloud OTEL configuration loaded!"
        echo ""
        echo "Available commands:"
        echo "  source $0 validate       - Validate configuration"
        echo "  source $0 examples       - Show usage examples"
        echo "  source $0 troubleshoot   - Show troubleshooting tips"
        echo "  source $0 agent-config   - Create Grafana Agent config"
        echo "  source $0 docker-compose - Create Docker Compose file"
        echo "  source $0 test           - Run test command"
        echo ""
        echo "Current configuration:"
        echo "  Service: $OTEL_SERVICE_NAME"
        echo "  Instance: $GRAFANA_CLOUD_INSTANCE"
        echo "  Region: $GRAFANA_CLOUD_REGION"
        echo "  Sampling: $OTEL_SAMPLING_RATE"
        echo ""
        ;;
esac

# Usage:
# 1. Edit GRAFANA_CLOUD_USER_ID, GRAFANA_CLOUD_API_TOKEN, and region above
# 2. Source this script: source grafana-cloud-config.sh
# 3. Validate: source grafana-cloud-config.sh validate
# 4. Run tdcli commands with tracing enabled
# 5. View traces at https://your-instance.grafana.net/explore
