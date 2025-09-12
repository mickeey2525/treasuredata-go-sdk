#!/bin/bash

# OpenTelemetry Setup Validation Script for tdcli
# This script helps validate your OTEL configuration before running tdcli commands

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Validation functions
validate_basic_config() {
    log_info "Validating basic OTEL configuration..."
    
    if [ "$OTEL_ENABLED" = "true" ]; then
        log_success "OTEL is enabled"
    else
        log_error "OTEL is not enabled (OTEL_ENABLED=$OTEL_ENABLED)"
        log_info "Set: export OTEL_ENABLED=true"
        return 1
    fi
    
    if [ -n "$OTEL_SERVICE_NAME" ]; then
        log_success "Service name is set: $OTEL_SERVICE_NAME"
    else
        log_warning "Service name not set, using default"
        log_info "Consider setting: export OTEL_SERVICE_NAME=tdcli"
    fi
    
    if [ -n "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" ]; then
        log_success "Trace endpoint configured: $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
    else
        log_error "Trace endpoint not configured"
        log_info "Set: export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces"
        return 1
    fi
    
    return 0
}

validate_sampling() {
    log_info "Validating sampling configuration..."
    
    if [ -n "$OTEL_SAMPLING_RATE" ]; then
        # Check if sampling rate is a valid number between 0 and 1
        if echo "$OTEL_SAMPLING_RATE" | grep -qE '^[0-1](\.[0-9]+)?$'; then
            if [ "$OTEL_SAMPLING_RATE" = "0" ] || [ "$OTEL_SAMPLING_RATE" = "0.0" ]; then
                log_warning "Sampling rate is 0 - no traces will be generated"
            elif echo "$OTEL_SAMPLING_RATE" | grep -qE '^1(\.0+)?$'; then
                log_warning "Sampling rate is 100% - consider reducing for production"
            else
                log_success "Sampling rate configured: ${OTEL_SAMPLING_RATE} (${OTEL_SAMPLING_RATE%.*}%)"
            fi
        else
            log_error "Invalid sampling rate: $OTEL_SAMPLING_RATE (must be between 0.0 and 1.0)"
            return 1
        fi
    else
        log_warning "Sampling rate not set, using default (1.0)"
    fi
    
    return 0
}

test_endpoint_connectivity() {
    log_info "Testing endpoint connectivity..."
    
    local endpoint="$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
    
    # Extract host and port from endpoint
    local host_port=$(echo "$endpoint" | sed -E 's|https?://([^/]+).*|\1|')
    local host=$(echo "$host_port" | cut -d: -f1)
    local port=$(echo "$host_port" | cut -d: -f2)
    
    # Default ports
    if [ "$port" = "$host" ]; then
        if echo "$endpoint" | grep -q "https://"; then
            port=443
        else
            port=80
        fi
    fi
    
    # Test basic connectivity
    if command -v nc >/dev/null 2>&1; then
        if nc -z "$host" "$port" 2>/dev/null; then
            log_success "Endpoint is reachable: $host:$port"
        else
            log_error "Cannot reach endpoint: $host:$port"
            log_info "Check if your collector/platform is running"
            return 1
        fi
    elif command -v telnet >/dev/null 2>&1; then
        if timeout 5 telnet "$host" "$port" </dev/null >/dev/null 2>&1; then
            log_success "Endpoint is reachable: $host:$port"
        else
            log_error "Cannot reach endpoint: $host:$port"
            return 1
        fi
    else
        log_warning "Cannot test connectivity (nc or telnet not available)"
    fi
    
    # Test HTTP endpoint
    if command -v curl >/dev/null 2>&1; then
        local response=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d '{"resourceSpans":[]}' \
            "$endpoint" 2>/dev/null || echo "000")
        
        case "$response" in
            200|202)
                log_success "Endpoint accepts OTLP data (HTTP $response)"
                ;;
            401|403)
                log_error "Authentication failed (HTTP $response)"
                log_info "Check your OTEL_EXPORTER_OTLP_HEADERS configuration"
                return 1
                ;;
            404)
                log_error "Endpoint not found (HTTP $response)"
                log_info "Verify the endpoint URL is correct"
                return 1
                ;;
            000)
                log_warning "Could not test HTTP endpoint (connection failed)"
                ;;
            *)
                log_warning "Unexpected response from endpoint (HTTP $response)"
                ;;
        esac
    else
        log_warning "Cannot test HTTP endpoint (curl not available)"
    fi
    
    return 0
}

validate_authentication() {
    log_info "Validating authentication configuration..."
    
    if [ -n "$OTEL_EXPORTER_OTLP_HEADERS" ]; then
        log_success "Authentication headers configured"
        
        # Check header format
        if echo "$OTEL_EXPORTER_OTLP_HEADERS" | grep -qE '^[^=]+=.*(,[^=]+=.*)*$'; then
            log_success "Header format appears valid"
        else
            log_error "Invalid header format: $OTEL_EXPORTER_OTLP_HEADERS"
            log_info "Format should be: key1=value1,key2=value2"
            return 1
        fi
        
        # Check for common authentication patterns
        if echo "$OTEL_EXPORTER_OTLP_HEADERS" | grep -qi "authorization="; then
            log_success "Authorization header detected"
        elif echo "$OTEL_EXPORTER_OTLP_HEADERS" | grep -qi "x-honeycomb-team="; then
            log_success "Honeycomb authentication detected"
        elif echo "$OTEL_EXPORTER_OTLP_HEADERS" | grep -qi "dd-api-key="; then
            log_success "Datadog authentication detected"
        elif echo "$OTEL_EXPORTER_OTLP_HEADERS" | grep -qi "api-key="; then
            log_success "API key authentication detected"
        else
            log_warning "No recognized authentication pattern found"
        fi
    else
        log_warning "No authentication headers configured"
        log_info "This is OK for local development but required for cloud platforms"
    fi
    
    return 0
}

validate_performance_settings() {
    log_info "Validating performance settings..."
    
    # Check batch settings
    if [ -n "$OTEL_BSP_MAX_EXPORT_BATCH_SIZE" ]; then
        if [ "$OTEL_BSP_MAX_EXPORT_BATCH_SIZE" -gt 2048 ]; then
            log_warning "Large batch size may cause memory issues: $OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
        else
            log_success "Batch size configured: $OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
        fi
    fi
    
    if [ -n "$OTEL_BSP_SCHEDULE_DELAY" ]; then
        if [ "$OTEL_BSP_SCHEDULE_DELAY" -lt 1000 ]; then
            log_warning "Very frequent exports may impact performance: ${OTEL_BSP_SCHEDULE_DELAY}ms"
        else
            log_success "Export delay configured: ${OTEL_BSP_SCHEDULE_DELAY}ms"
        fi
    fi
    
    if [ -n "$OTEL_EXPORTER_OTLP_TIMEOUT" ]; then
        if [ "$OTEL_EXPORTER_OTLP_TIMEOUT" -lt 5000 ]; then
            log_warning "Short timeout may cause export failures: ${OTEL_EXPORTER_OTLP_TIMEOUT}ms"
        else
            log_success "Export timeout configured: ${OTEL_EXPORTER_OTLP_TIMEOUT}ms"
        fi
    fi
    
    return 0
}

test_tdcli_integration() {
    log_info "Testing tdcli OTEL integration..."
    
    if ! command -v tdcli >/dev/null 2>&1; then
        log_error "tdcli command not found"
        log_info "Make sure tdcli is installed and in your PATH"
        return 1
    fi
    
    # Test with help command (minimal impact)
    log_info "Running test command: tdcli --help"
    if tdcli --help >/dev/null 2>&1; then
        log_success "tdcli executed successfully"
    else
        log_error "tdcli execution failed"
        return 1
    fi
    
    # Check if debug logging shows OTEL activity
    if [ "$OTEL_LOG_LEVEL" = "debug" ]; then
        log_info "Debug logging enabled - check output for OTEL messages"
    else
        log_info "Enable debug logging with: export OTEL_LOG_LEVEL=debug"
    fi
    
    return 0
}

show_configuration_summary() {
    echo ""
    log_info "Configuration Summary:"
    echo "  OTEL Enabled: ${OTEL_ENABLED:-false}"
    echo "  Service Name: ${OTEL_SERVICE_NAME:-<not set>}"
    echo "  Trace Endpoint: ${OTEL_EXPORTER_OTLP_TRACES_ENDPOINT:-<not set>}"
    echo "  Metric Endpoint: ${OTEL_EXPORTER_OTLP_METRICS_ENDPOINT:-<not set>}"
    echo "  Sampling Rate: ${OTEL_SAMPLING_RATE:-1.0}"
    echo "  Headers: ${OTEL_EXPORTER_OTLP_HEADERS:+<configured>}"
    echo "  Insecure: ${OTEL_EXPORTER_OTLP_INSECURE:-false}"
    echo ""
}

show_next_steps() {
    echo ""
    log_info "Next Steps:"
    echo "  1. Run tdcli commands normally - tracing will be automatic"
    echo "  2. Check your observability platform for traces"
    echo "  3. Look for service name: ${OTEL_SERVICE_NAME:-tdcli}"
    echo ""
    
    # Platform-specific URLs
    if echo "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" | grep -q "localhost:16686"; then
        echo "  Jaeger UI: http://localhost:16686"
    elif echo "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" | grep -q "honeycomb.io"; then
        echo "  Honeycomb UI: https://ui.honeycomb.io"
    elif echo "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" | grep -q "datadoghq"; then
        echo "  Datadog APM: https://app.datadoghq.com/apm"
    elif echo "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" | grep -q "grafana.net"; then
        echo "  Grafana Cloud: https://your-instance.grafana.net/explore"
    fi
    echo ""
}

# Main validation function
main() {
    echo "🔍 OpenTelemetry Setup Validation for tdcli"
    echo "=============================================="
    echo ""
    
    local errors=0
    
    # Run all validations
    validate_basic_config || ((errors++))
    echo ""
    
    validate_sampling || ((errors++))
    echo ""
    
    test_endpoint_connectivity || ((errors++))
    echo ""
    
    validate_authentication || ((errors++))
    echo ""
    
    validate_performance_settings || ((errors++))
    echo ""
    
    test_tdcli_integration || ((errors++))
    echo ""
    
    # Show summary
    show_configuration_summary
    
    # Final result
    if [ $errors -eq 0 ]; then
        log_success "All validations passed! Your OTEL setup looks good."
        show_next_steps
        exit 0
    else
        log_error "Found $errors issue(s) with your OTEL configuration."
        echo ""
        log_info "Common fixes:"
        echo "  - export OTEL_ENABLED=true"
        echo "  - export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces"
        echo "  - Start your collector/platform (e.g., docker-compose up jaeger)"
        echo "  - Check authentication headers for cloud platforms"
        echo ""
        exit 1
    fi
}

# Handle command line arguments
case "${1:-}" in
    "--help"|"-h")
        echo "Usage: $0 [options]"
        echo ""
        echo "Validates your OpenTelemetry configuration for tdcli."
        echo ""
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --summary, -s  Show configuration summary only"
        echo "  --quiet, -q    Suppress info messages"
        echo ""
        echo "Environment variables checked:"
        echo "  OTEL_ENABLED"
        echo "  OTEL_SERVICE_NAME"
        echo "  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
        echo "  OTEL_EXPORTER_OTLP_HEADERS"
        echo "  OTEL_SAMPLING_RATE"
        echo "  And other OTEL_* variables"
        echo ""
        exit 0
        ;;
    "--summary"|"-s")
        show_configuration_summary
        exit 0
        ;;
    "--quiet"|"-q")
        # Redirect info messages to /dev/null
        exec 3>&1
        log_info() { :; }
        ;;
esac

# Run main validation
main
