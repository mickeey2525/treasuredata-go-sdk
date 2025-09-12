#!/bin/bash

# Honeycomb OpenTelemetry Configuration for tdcli
# This script sets up environment variables for Honeycomb integration

# =============================================================================
# Honeycomb Configuration
# =============================================================================

# Replace with your actual Honeycomb API key
HONEYCOMB_API_KEY="your_honeycomb_api_key_here"

# Honeycomb team/environment (for Honeycomb Environments)
HONEYCOMB_ENVIRONMENT="production"

# Dataset name (for Honeycomb Classic)
HONEYCOMB_DATASET="tdcli"

# =============================================================================
# Basic OTEL Configuration
# =============================================================================

export OTEL_ENABLED=true
export OTEL_SERVICE_NAME="tdcli"
export OTEL_SERVICE_VERSION="1.0.0"

# =============================================================================
# Honeycomb Endpoints and Authentication
# =============================================================================

# For Honeycomb Environments (recommended)
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="https://api.honeycomb.io/v1/traces"
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="https://api.honeycomb.io/v1/metrics"

# Authentication headers for Honeycomb Environments
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=${HONEYCOMB_API_KEY}"

# For Honeycomb Classic (legacy)
# export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=${HONEYCOMB_API_KEY},x-honeycomb-dataset=${HONEYCOMB_DATASET}"

# =============================================================================
# Resource Attributes
# =============================================================================

export OTEL_RESOURCE_ATTRIBUTES="service.name=tdcli,service.version=1.0.0,deployment.environment=${HONEYCOMB_ENVIRONMENT},service.namespace=treasure-data"

# =============================================================================
# Sampling Configuration
# =============================================================================

# Adjust sampling rate based on your needs
# 1.0 = 100% sampling (good for development)
# 0.1 = 10% sampling (good for production)
# 0.01 = 1% sampling (good for high-volume production)
export OTEL_SAMPLING_RATE=0.1

# =============================================================================
# Performance Tuning
# =============================================================================

# Batch configuration for better performance
export OTEL_BSP_SCHEDULE_DELAY=2000  # 2 seconds
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=512
export OTEL_BSP_MAX_QUEUE_SIZE=2048

# Export timeout
export OTEL_EXPORTER_OTLP_TIMEOUT=10000  # 10 seconds

# =============================================================================
# Validation and Testing
# =============================================================================

validate_config() {
    echo "Validating Honeycomb configuration..."
    
    # Check if API key is set
    if [ "$HONEYCOMB_API_KEY" = "your_honeycomb_api_key_here" ]; then
        echo "❌ Please set your actual Honeycomb API key"
        return 1
    fi
    
    # Test connectivity to Honeycomb
    echo "Testing connectivity to Honeycomb..."
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "x-honeycomb-team: ${HONEYCOMB_API_KEY}" \
        "https://api.honeycomb.io/1/auth")
    
    if [ "$response" = "200" ]; then
        echo "✅ Successfully authenticated with Honeycomb"
    else
        echo "❌ Failed to authenticate with Honeycomb (HTTP $response)"
        echo "Please check your API key and network connectivity"
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
    echo "   OTEL_RESOURCE_ATTRIBUTES=\"\$OTEL_RESOURCE_ATTRIBUTES,user.id=john.doe\" \\"
    echo "   tdcli queries submit --database mydb --query 'SELECT COUNT(*) FROM users'"
    echo ""
    echo "3. Bulk import with high sampling:"
    echo "   OTEL_SAMPLING_RATE=1.0 \\"
    echo "   tdcli bulk-import --database mydb --table mytable --file data.csv"
    echo ""
    echo "4. View traces in Honeycomb:"
    echo "   https://ui.honeycomb.io/your-team/datasets/tdcli"
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
    echo ""
    echo "2. Test API key:"
    echo "   curl -H \"x-honeycomb-team: \$HONEYCOMB_API_KEY\" https://api.honeycomb.io/1/auth"
    echo ""
    echo "3. Enable debug logging:"
    echo "   OTEL_LOG_LEVEL=debug tdcli queries list"
    echo ""
    echo "4. Check Honeycomb ingestion:"
    echo "   - Go to https://ui.honeycomb.io"
    echo "   - Check the 'Recent Traces' section"
    echo "   - Look for service.name=tdcli"
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
    "test")
        validate_config && {
            echo ""
            echo "Running test command with tracing..."
            tdcli --help
            echo ""
            echo "Check Honeycomb UI for traces: https://ui.honeycomb.io"
        }
        ;;
    *)
        echo "Honeycomb OTEL configuration loaded!"
        echo ""
        echo "Available commands:"
        echo "  source $0 validate      - Validate configuration"
        echo "  source $0 examples      - Show usage examples"
        echo "  source $0 troubleshoot  - Show troubleshooting tips"
        echo "  source $0 test          - Run test command"
        echo ""
        echo "Current configuration:"
        echo "  Service: $OTEL_SERVICE_NAME"
        echo "  Endpoint: $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
        echo "  Sampling: $OTEL_SAMPLING_RATE"
        echo ""
        ;;
esac

# Usage:
# 1. Edit HONEYCOMB_API_KEY above
# 2. Source this script: source honeycomb-config.sh
# 3. Validate: source honeycomb-config.sh validate
# 4. Run tdcli commands with tracing enabled
