package otel

import (
	"fmt"
	"log"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// SecurityConfig holds security-related configuration options
type SecurityConfig struct {
	// AllowInsecureEndpoints allows HTTP endpoints (should be false in production)
	AllowInsecureEndpoints bool
	// LogSecurityWarnings enables logging of security-related warnings
	LogSecurityWarnings bool
	// StrictCredentialFiltering enables strict filtering of potential credentials
	StrictCredentialFiltering bool
	// MaxAttributeValueLength for security-sensitive attributes
	MaxSecureAttributeLength int
}

// DefaultSecurityConfig returns a security configuration with secure defaults
func DefaultSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		AllowInsecureEndpoints:    false,
		LogSecurityWarnings:       true,
		StrictCredentialFiltering: true,
		MaxSecureAttributeLength:  256, // Shorter limit for potentially sensitive data
	}
}

// SecureCredentialHandler provides methods for secure handling of credentials and sensitive data
type SecureCredentialHandler struct {
	config    *SecurityConfig
	sanitizer *DataSanitizer
}

// NewSecureCredentialHandler creates a new secure credential handler
func NewSecureCredentialHandler(securityConfig *SecurityConfig, sanitizerConfig *SanitizationConfig) *SecureCredentialHandler {
	if securityConfig == nil {
		securityConfig = DefaultSecurityConfig()
	}

	sanitizer := NewDataSanitizer(sanitizerConfig)

	return &SecureCredentialHandler{
		config:    securityConfig,
		sanitizer: sanitizer,
	}
}

// ValidateAndSanitizeEndpoint validates an endpoint for security and sanitizes it for logging
func (h *SecureCredentialHandler) ValidateAndSanitizeEndpoint(endpoint string) (string, error) {
	if endpoint == "" {
		return "", nil
	}

	// Parse the URL
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL: %w", err)
	}

	// Security validation
	if u.Scheme == "http" && !h.config.AllowInsecureEndpoints {
		if h.isLocalhost(u.Hostname()) {
			if h.config.LogSecurityWarnings {
				log.Printf("WARNING: Using insecure HTTP endpoint for localhost: %s", endpoint)
			}
		} else {
			return "", fmt.Errorf("insecure HTTP endpoint not allowed: %s (use HTTPS or enable AllowInsecureEndpoints)", endpoint)
		}
	}

	// Sanitize for logging/display
	sanitized := h.sanitizer.SanitizeURL(endpoint)
	return sanitized, nil
}

// isLocalhost checks if a hostname is localhost
func (h *SecureCredentialHandler) isLocalhost(hostname string) bool {
	return hostname == "localhost" ||
		hostname == "127.0.0.1" ||
		hostname == "::1" ||
		strings.HasSuffix(hostname, ".localhost")
}

// SanitizeSpanAttributes removes or redacts sensitive information from span attributes
func (h *SecureCredentialHandler) SanitizeSpanAttributes(attrs []attribute.KeyValue) []attribute.KeyValue {
	if len(attrs) == 0 {
		return attrs
	}

	sanitized := make([]attribute.KeyValue, 0, len(attrs))

	for _, attr := range attrs {
		key := string(attr.Key)
		value := attr.Value.AsString()

		// Check if this attribute should be completely redacted
		if h.shouldRedactAttribute(key) {
			sanitized = append(sanitized, attribute.String(key, "[REDACTED]"))
			continue
		}

		// Apply content-specific sanitization
		sanitizedValue := h.sanitizeAttributeValue(key, value)

		// Apply length limits for potentially sensitive attributes
		if h.isPotentiallySensitive(key) && len(sanitizedValue) > h.config.MaxSecureAttributeLength {
			sanitizedValue = sanitizedValue[:h.config.MaxSecureAttributeLength-len("...[truncated]")] + "...[truncated]"
		}

		sanitized = append(sanitized, attribute.String(key, sanitizedValue))
	}

	// Use the sanitizer for final processing
	return h.sanitizer.SanitizeAttributes(sanitized)
}

// shouldRedactAttribute determines if an attribute should be completely redacted
func (h *SecureCredentialHandler) shouldRedactAttribute(key string) bool {
	if !h.config.StrictCredentialFiltering {
		return false
	}

	lowerKey := strings.ToLower(key)

	// Attributes that should be completely redacted
	redactPatterns := []string{
		"api_key", "apikey", "api-key",
		"token", "access_token", "refresh_token", "bearer_token",
		"password", "passwd", "pwd",
		"secret", "client_secret", "auth_secret",
		"authorization",
		"credential", "credentials",
		"private_key", "privatekey",
		"session_id", "sessionid",
		"cookie", "cookies",
	}

	for _, pattern := range redactPatterns {
		if strings.Contains(lowerKey, pattern) {
			return true
		}
	}

	return false
}

// isPotentiallySensitive checks if an attribute might contain sensitive data
func (h *SecureCredentialHandler) isPotentiallySensitive(key string) bool {
	lowerKey := strings.ToLower(key)

	// Attributes that might contain sensitive data but shouldn't be completely redacted
	sensitivePatterns := []string{
		"user", "username", "userid", "user_id",
		"email", "mail",
		"phone", "telephone",
		"address", "location",
		"firstname", "lastname", "user.name", // More specific name patterns
		"id", "identifier",
		"header", "headers",
		"param", "parameter",
		"query", "statement",
		"url", "uri", "endpoint",
	}

	for _, pattern := range sensitivePatterns {
		if strings.Contains(lowerKey, pattern) {
			// Exclude common service/system attributes that contain these words
			if strings.HasPrefix(lowerKey, "service.") ||
				strings.HasPrefix(lowerKey, "resource.") ||
				strings.HasPrefix(lowerKey, "telemetry.") ||
				strings.HasPrefix(lowerKey, "otel.") {
				continue
			}
			return true
		}
	}

	return false
}

// sanitizeAttributeValue applies content-specific sanitization
func (h *SecureCredentialHandler) sanitizeAttributeValue(key, value string) string {
	lowerKey := strings.ToLower(key)

	// SQL statements
	if strings.Contains(lowerKey, "statement") || strings.Contains(lowerKey, "query") || strings.Contains(lowerKey, "sql") {
		return h.sanitizer.SanitizeSQL(value)
	}

	// URLs and endpoints
	if strings.Contains(lowerKey, "url") || strings.Contains(lowerKey, "endpoint") || strings.Contains(lowerKey, "uri") {
		return h.sanitizer.SanitizeURL(value)
	}

	// Email addresses - partially redact
	if strings.Contains(lowerKey, "email") || strings.Contains(lowerKey, "mail") {
		return h.sanitizeEmail(value)
	}

	// User IDs and identifiers - partially redact if they look like UUIDs or long IDs
	if (strings.Contains(lowerKey, "id") || strings.Contains(lowerKey, "identifier")) && len(value) > 16 {
		return h.sanitizeIdentifier(value)
	}

	return value
}

// sanitizeEmail partially redacts email addresses
func (h *SecureCredentialHandler) sanitizeEmail(email string) string {
	if email == "" || !strings.Contains(email, "@") {
		return email
	}

	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return email
	}

	username := parts[0]
	domain := parts[1]

	// Redact most of the username but keep first and last character if long enough
	if len(username) <= 2 {
		return "[REDACTED]@" + domain
	} else if len(username) <= 4 {
		return username[:1] + "*@" + domain
	} else {
		return username[:1] + "***" + username[len(username)-1:] + "@" + domain
	}
}

// sanitizeIdentifier partially redacts long identifiers
func (h *SecureCredentialHandler) sanitizeIdentifier(id string) string {
	if len(id) <= 8 {
		return id
	}

	// For UUIDs and long IDs, show first 4 and last 4 characters
	if len(id) >= 16 {
		return id[:4] + "****" + id[len(id)-4:]
	}

	// For shorter IDs, show first 2 and last 2
	return id[:2] + "****" + id[len(id)-2:]
}

// SanitizeError sanitizes error messages to remove sensitive information
func (h *SecureCredentialHandler) SanitizeError(err error) error {
	if err == nil {
		return nil
	}

	// Use the sanitizer's error sanitization
	sanitized := h.sanitizer.SanitizeError(err)

	// Additional security-specific sanitization
	errMsg := sanitized.Error()

	// Remove potential tokens from error messages
	// Simple string-based replacement for common patterns
	lowerErrMsg := strings.ToLower(errMsg)
	if strings.Contains(lowerErrMsg, "token=") {
		// Find and replace the actual token value
		parts := strings.Split(errMsg, "token=")
		if len(parts) > 1 {
			// Replace everything after "token=" until next space or end
			tokenPart := parts[1]
			spaceIndex := strings.Index(tokenPart, " ")
			if spaceIndex == -1 {
				errMsg = parts[0] + "token=[REDACTED]"
			} else {
				errMsg = parts[0] + "token=[REDACTED]" + tokenPart[spaceIndex:]
			}
		}
	}
	if strings.Contains(lowerErrMsg, "key=") {
		// Find and replace the actual key value
		parts := strings.Split(errMsg, "key=")
		if len(parts) > 1 {
			// Replace everything after "key=" until next space or end
			keyPart := parts[1]
			spaceIndex := strings.Index(keyPart, " ")
			if spaceIndex == -1 {
				errMsg = parts[0] + "key=[REDACTED]"
			} else {
				errMsg = parts[0] + "key=[REDACTED]" + keyPart[spaceIndex:]
			}
		}
	}

	return fmt.Errorf("%s", errMsg)
}

// ValidateConfiguration validates the security configuration
func (h *SecureCredentialHandler) ValidateConfiguration(config *OTELConfig) []string {
	var warnings []string

	if !h.config.LogSecurityWarnings {
		return warnings
	}

	// Check for insecure endpoints
	if config.TraceEndpoint != "" {
		if u, err := url.Parse(config.TraceEndpoint); err == nil {
			if u.Scheme == "http" && !h.isLocalhost(u.Hostname()) {
				warnings = append(warnings, fmt.Sprintf("Trace endpoint uses insecure HTTP: %s", config.TraceEndpoint))
			}
		}
	}

	if config.MetricEndpoint != "" {
		if u, err := url.Parse(config.MetricEndpoint); err == nil {
			if u.Scheme == "http" && !h.isLocalhost(u.Hostname()) {
				warnings = append(warnings, fmt.Sprintf("Metric endpoint uses insecure HTTP: %s", config.MetricEndpoint))
			}
		}
	}

	// Check for potentially sensitive headers
	for key, value := range config.Headers {
		lowerKey := strings.ToLower(key)
		if strings.Contains(lowerKey, "password") ||
			strings.Contains(lowerKey, "secret") ||
			(strings.Contains(lowerKey, "key") && !strings.Contains(lowerKey, "api")) {
			warnings = append(warnings, fmt.Sprintf("Header %q may contain sensitive data", key))
		}

		// Check for long header values that might contain tokens
		if len(value) > 100 && (strings.Contains(lowerKey, "auth") || strings.Contains(lowerKey, "token")) {
			warnings = append(warnings, fmt.Sprintf("Header %q has unusually long value, may contain token", key))
		}
	}

	return warnings
}

// GetSecurityConfig returns the current security configuration
func (h *SecureCredentialHandler) GetSecurityConfig() *SecurityConfig {
	return h.config
}
