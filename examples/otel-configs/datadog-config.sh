#!/bin/bash

# Datadog OpenTelemetry Configuration for tdcli
# This script sets up environment variables for Datadog APM integration

# =============================================================================
# Datadog Configuration
# =============================================================================

# Replace with your actual Datadog API key
DD_API_KEY="your_datadog_api_key_here"

# Datadog site (us1, us3, us5, eu1, ap1, etc.)
DD_SITE="datadoghq.com"

# Environment and service configuration
DD_ENV="production"
DD_SERVICE="tdcli"
DD_VERSION="1.0.0"

# =============================================================================
# Basic OTEL Configuration
# =============================================================================

export OTEL_ENABLED=true
export OTEL_SERVICE_NAME="$DD_SERVICE"
export OTEL_SERVICE_VERSION="$DD_VERSION"

# =============================================================================
# Datadog Agent Configuration (Recommended)
# =============================================================================

# Use local Datadog Agent (recommended for production)
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://localhost:4318/v1/traces"
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://localhost:4318/v1/metrics"

# Alternative: Direct to Datadog (requires API key in headers)
# export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="https://trace.agent.${DD_SITE}/v0.4/traces"
# export OTEL_EXPORTER_OTLP_HEADERS="dd-api-key=${DD_API_KEY}"

# =============================================================================
# Resource Attributes (Datadog Tags)
# =============================================================================

export OTEL_RESOURCE_ATTRIBUTES="service.name=${DD_SERVICE},service.version=${DD_VERSION},deployment.environment=${DD_ENV},service.namespace=treasure-data"

# =============================================================================
# Sampling Configuration
# =============================================================================

# Datadog recommends starting with 100% sampling and adjusting based on volume
export OTEL_SAMPLING_RATE=1.0

# For high-volume applications, consider lower sampling rates:
# export OTEL_SAMPLING_RATE=0.1  # 10% sampling

# =============================================================================
# Performance Tuning
# =============================================================================

# Batch configuration optimized for Datadog
export OTEL_BSP_SCHEDULE_DELAY=1000   # 1 second
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=100
export OTEL_BSP_MAX_QUEUE_SIZE=2048

# Export timeout
export OTEL_EXPORTER_OTLP_TIMEOUT=5000  # 5 seconds

# =============================================================================
# Datadog Agent Setup
# =============================================================================

setup_datadog_agent() {
    echo "Setting up Datadog Agent with OTLP support..."
    
    # Create Datadog Agent configuration directory
    mkdir -p ~/.datadog-agent
    
    # Create datadog.yaml configuration
    cat > ~/.datadog-agent/datadog.yaml << EOF
# Datadog Agent configuration for OTLP support
api_key: ${DD_API_KEY}
site: ${DD_SITE}

# Enable OTLP receiver
otlp_config:
  receiver:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

# APM configuration
apm_config:
  enabled: true
  env: ${DD_ENV}
  
# Logs configuration
logs_enabled: true
logs_config:
  container_collect_all: true

# Process monitoring
process_config:
  enabled: true

# Network monitoring
network_config:
  enabled: true
EOF

    echo "✅ Datadog Agent configuration created at ~/.datadog-agent/datadog.yaml"
}

# =============================================================================
# Docker Compose for Datadog Agent
# =============================================================================

create_docker_compose() {
    cat > docker-compose.datadog.yml << EOF
version: '3.8'

services:
  datadog-agent:
    image: gcr.io/datadoghq/agent:latest
    container_name: datadog-agent
    environment:
      - DD_API_KEY=${DD_API_KEY}
      - DD_SITE=${DD_SITE}
      - DD_APM_ENABLED=true
      - DD_APM_NON_LOCAL_TRAFFIC=true
      - DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT=0.0.0.0:4317
      - DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT=0.0.0.0:4318
      - DD_LOGS_ENABLED=true
      - DD_LOGS_CONFIG_CONTAINER_COLLECT_ALL=true
      - DD_PROCESS_AGENT_ENABLED=true
      - DD_ENV=${DD_ENV}
    ports:
      - "4317:4317"  # OTLP gRPC
      - "4318:4318"  # OTLP HTTP
      - "8125:8125/udp"  # DogStatsD
      - "8126:8126"  # APM
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - /proc/:/host/proc/:ro
      - /opt/datadog-agent/run:/opt/datadog-agent/run:rw
      - /sys/fs/cgroup/:/host/sys/fs/cgroup:ro
    networks:
      - datadog-network

networks:
  datadog-network:
    driver: bridge
EOF

    echo "✅ Docker Compose configuration created: docker-compose.datadog.yml"
    echo "   Start with: docker-compose -f docker-compose.datadog.yml up -d"
}

# =============================================================================
# Validation and Testing
# =============================================================================

validate_config() {
    echo "Validating Datadog configuration..."
    
    # Check if API key is set
    if [ "$DD_API_KEY" = "your_datadog_api_key_here" ]; then
        echo "❌ Please set your actual Datadog API key"
        return 1
    fi
    
    # Test connectivity to Datadog Agent
    echo "Testing connectivity to Datadog Agent..."
    if curl -s -f http://localhost:4318/v1/traces > /dev/null 2>&1; then
        echo "✅ Datadog Agent OTLP endpoint is accessible"
    else
        echo "⚠️  Datadog Agent OTLP endpoint not accessible"
        echo "   Make sure Datadog Agent is running with OTLP enabled"
        echo "   Run: docker-compose -f docker-compose.datadog.yml up -d"
    fi
    
    # Test Datadog API connectivity
    echo "Testing Datadog API connectivity..."
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "DD-API-KEY: ${DD_API_KEY}" \
        "https://api.${DD_SITE}/api/v1/validate")
    
    if [ "$response" = "200" ]; then
        echo "✅ Successfully authenticated with Datadog API"
    else
        echo "❌ Failed to authenticate with Datadog API (HTTP $response)"
        echo "Please check your API key and DD_SITE configuration"
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
    echo "2. Submit query with custom tags:"
    echo "   OTEL_RESOURCE_ATTRIBUTES=\"\$OTEL_RESOURCE_ATTRIBUTES,user.id=john.doe,team=data-engineering\" \\"
    echo "   tdcli queries submit --database mydb --query 'SELECT COUNT(*) FROM users'"
    echo ""
    echo "3. Bulk import with detailed tracing:"
    echo "   OTEL_SAMPLING_RATE=1.0 \\"
    echo "   tdcli bulk-import --database mydb --table mytable --file data.csv"
    echo ""
    echo "4. View traces in Datadog:"
    echo "   https://app.${DD_SITE}/apm/traces?env=${DD_ENV}&service=${DD_SERVICE}"
    echo ""
    echo "5. Custom dashboard queries:"
    echo "   - Service overview: service:${DD_SERVICE} env:${DD_ENV}"
    echo "   - Error traces: service:${DD_SERVICE} status:error"
    echo "   - Slow queries: service:${DD_SERVICE} @duration:>5s"
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
    echo "   env | grep DD_"
    echo ""
    echo "2. Test Datadog Agent status:"
    echo "   curl http://localhost:4318/v1/traces"
    echo "   docker logs datadog-agent"
    echo ""
    echo "3. Validate API key:"
    echo "   curl -H \"DD-API-KEY: \$DD_API_KEY\" https://api.${DD_SITE}/api/v1/validate"
    echo ""
    echo "4. Enable debug logging:"
    echo "   OTEL_LOG_LEVEL=debug tdcli queries list"
    echo ""
    echo "5. Check Datadog APM:"
    echo "   - Go to https://app.${DD_SITE}/apm/services"
    echo "   - Look for service: ${DD_SERVICE}"
    echo "   - Check environment: ${DD_ENV}"
    echo ""
    echo "6. Common issues:"
    echo "   - Agent not running: docker-compose -f docker-compose.datadog.yml up -d"
    echo "   - Wrong DD_SITE: Check your Datadog account region"
    echo "   - Firewall blocking: Ensure ports 4317/4318 are open"
    echo "   - API key invalid: Verify in Datadog Organization Settings"
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
    "setup-agent")
        setup_datadog_agent
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
            echo "Check Datadog APM for traces: https://app.${DD_SITE}/apm/traces?env=${DD_ENV}&service=${DD_SERVICE}"
        }
        ;;
    *)
        echo "Datadog OTEL configuration loaded!"
        echo ""
        echo "Available commands:"
        echo "  source $0 validate       - Validate configuration"
        echo "  source $0 examples       - Show usage examples"
        echo "  source $0 troubleshoot   - Show troubleshooting tips"
        echo "  source $0 setup-agent    - Create Datadog Agent config"
        echo "  source $0 docker-compose - Create Docker Compose file"
        echo "  source $0 test           - Run test command"
        echo ""
        echo "Current configuration:"
        echo "  Service: $OTEL_SERVICE_NAME"
        echo "  Environment: $DD_ENV"
        echo "  Endpoint: $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
        echo "  Sampling: $OTEL_SAMPLING_RATE"
        echo ""
        ;;
esac

# Usage:
# 1. Edit DD_API_KEY and DD_SITE above
# 2. Source this script: source datadog-config.sh
# 3. Set up agent: source datadog-config.sh setup-agent
# 4. Validate: source datadog-config.sh validate
# 5. Run tdcli commands with tracing enabled
