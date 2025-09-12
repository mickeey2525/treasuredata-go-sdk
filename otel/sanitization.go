package otel

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

const (
	// MaxAttributeValueLength is the maximum length for span attribute values
	MaxAttributeValueLength = 4096
	// MaxAttributeKeyLength is the maximum length for span attribute keys
	MaxAttributeKeyLength = 256
	// TruncationSuffix is appended to truncated values
	TruncationSuffix = "...[truncated]"
)

var (
	// SQL literal patterns to sanitize
	sqlStringLiteralRegex = regexp.MustCompile(`'([^'\\]|\\.)*'`)
	sqlNumberLiteralRegex = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
	sqlHexLiteralRegex    = regexp.MustCompile(`0x[0-9a-fA-F]+`)

	// Sensitive parameter patterns in URLs
	sensitiveParamRegex = regexp.MustCompile(`(?i)(api[_-]?key|token|password|secret|auth|credential|bearer)=([^&\s]+)`)

	// Sensitive header patterns
	sensitiveHeaderRegex = regexp.MustCompile(`(?i)(authorization|api[_-]?key|token|password|secret|auth|credential|bearer)`)
)

// SanitizationConfig holds configuration for data sanitization
type SanitizationConfig struct {
	// EnableSQLSanitization controls whether SQL queries are sanitized
	EnableSQLSanitization bool
	// EnableURLSanitization controls whether URLs are sanitized
	EnableURLSanitization bool
	// EnableAttributeSizeLimiting controls whether attribute values are size-limited
	EnableAttributeSizeLimiting bool
	// MaxAttributeValueLength overrides the default max attribute value length
	MaxAttributeValueLength int
	// MaxAttributeKeyLength overrides the default max attribute key length
	MaxAttributeKeyLength int
	// CustomSensitiveParams additional parameter names to sanitize in URLs
	CustomSensitiveParams []string
	// CustomSensitiveHeaders additional header names to sanitize
	CustomSensitiveHeaders []string
}

// DefaultSanitizationConfig returns a sanitization configuration with secure defaults
func DefaultSanitizationConfig() *SanitizationConfig {
	return &SanitizationConfig{
		EnableSQLSanitization:       true,
		EnableURLSanitization:       true,
		EnableAttributeSizeLimiting: true,
		MaxAttributeValueLength:     MaxAttributeValueLength,
		MaxAttributeKeyLength:       MaxAttributeKeyLength,
		CustomSensitiveParams:       []string{},
		CustomSensitiveHeaders:      []string{},
	}
}

// DataSanitizer provides methods for sanitizing sensitive data in telemetry
type DataSanitizer struct {
	config *SanitizationConfig
}

// NewDataSanitizer creates a new data sanitizer with the given configuration
func NewDataSanitizer(config *SanitizationConfig) *DataSanitizer {
	if config == nil {
		config = DefaultSanitizationConfig()
	}
	return &DataSanitizer{config: config}
}

// SanitizeSQL removes sensitive literals from SQL queries while preserving structure
func (s *DataSanitizer) SanitizeSQL(query string) string {
	if !s.config.EnableSQLSanitization || query == "" {
		return query
	}

	sanitized := query

	// Replace string literals with placeholder
	sanitized = sqlStringLiteralRegex.ReplaceAllString(sanitized, "'?'")

	// Replace numeric literals with placeholder
	sanitized = sqlNumberLiteralRegex.ReplaceAllString(sanitized, "?")

	// Replace hex literals with placeholder
	sanitized = sqlHexLiteralRegex.ReplaceAllString(sanitized, "0x?")

	// Normalize whitespace
	sanitized = strings.Join(strings.Fields(sanitized), " ")

	return sanitized
}

// SanitizeURL removes sensitive parameters and credentials from URLs
func (s *DataSanitizer) SanitizeURL(rawURL string) string {
	if !s.config.EnableURLSanitization || rawURL == "" {
		return rawURL
	}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		// If URL parsing fails, try basic string sanitization
		return s.sanitizeURLString(rawURL)
	}

	// Sanitize query parameters
	if parsedURL.RawQuery != "" {
		values := parsedURL.Query()
		modified := false
		for key := range values {
			if s.isSensitiveParam(key) {
				values.Set(key, "[REDACTED]")
				modified = true
			}
		}
		if modified {
			parsedURL.RawQuery = values.Encode()
		}
	}

	// Remove user info (credentials in URL)
	if parsedURL.User != nil {
		parsedURL.User = url.UserPassword("[REDACTED]", "[REDACTED]")
	}

	return parsedURL.String()
}

// sanitizeURLString performs basic string-based URL sanitization as fallback
func (s *DataSanitizer) sanitizeURLString(rawURL string) string {
	// Use regex to replace sensitive parameters
	sanitized := sensitiveParamRegex.ReplaceAllString(rawURL, "${1}=[REDACTED]")

	// Check for custom sensitive parameters
	for _, param := range s.config.CustomSensitiveParams {
		pattern := fmt.Sprintf(`(?i)%s=([^&\s]+)`, regexp.QuoteMeta(param))
		regex := regexp.MustCompile(pattern)
		sanitized = regex.ReplaceAllString(sanitized, param+"=[REDACTED]")
	}

	return sanitized
}

// isSensitiveParam checks if a parameter name is considered sensitive
func (s *DataSanitizer) isSensitiveParam(paramName string) bool {
	lowerParam := strings.ToLower(paramName)

	// Check against built-in sensitive patterns
	sensitivePatterns := []string{
		"api_key", "apikey", "api-key",
		"token", "auth_token", "auth-token",
		"password", "passwd", "pwd",
		"secret", "auth_secret", "auth-secret",
		"authorization", "auth",
		"credential", "credentials",
		"bearer",
	}

	for _, pattern := range sensitivePatterns {
		if lowerParam == pattern {
			return true
		}
	}

	// Check against custom sensitive parameters
	for _, sensitive := range s.config.CustomSensitiveParams {
		if strings.ToLower(sensitive) == lowerParam {
			return true
		}
	}

	return false
}

// SanitizeHeaders removes sensitive values from HTTP headers
func (s *DataSanitizer) SanitizeHeaders(headers map[string]string) map[string]string {
	if !s.config.EnableURLSanitization || len(headers) == 0 {
		return headers
	}

	sanitized := make(map[string]string)
	for key, value := range headers {
		if s.isSensitiveHeader(key) {
			sanitized[key] = "[REDACTED]"
		} else {
			sanitized[key] = value
		}
	}

	return sanitized
}

// isSensitiveHeader checks if a header name is considered sensitive
func (s *DataSanitizer) isSensitiveHeader(headerName string) bool {
	// Check against built-in sensitive patterns
	if sensitiveHeaderRegex.MatchString(headerName) {
		return true
	}

	// Check against custom sensitive headers
	lowerHeader := strings.ToLower(headerName)
	for _, sensitive := range s.config.CustomSensitiveHeaders {
		if strings.ToLower(sensitive) == lowerHeader {
			return true
		}
	}

	return false
}

// SanitizeAttributes applies size limits and content filtering to span attributes
func (s *DataSanitizer) SanitizeAttributes(attrs []attribute.KeyValue) []attribute.KeyValue {
	if len(attrs) == 0 {
		return attrs
	}

	sanitized := make([]attribute.KeyValue, 0, len(attrs))

	for _, attr := range attrs {
		key := string(attr.Key)
		value := attr.Value.AsString()

		// Apply content-specific sanitization first (before truncation)
		value = s.sanitizeAttributeValue(key, value)

		// Truncate key if too long and size limiting is enabled
		if s.config.EnableAttributeSizeLimiting && len(key) > s.config.MaxAttributeKeyLength {
			if s.config.MaxAttributeKeyLength > len(TruncationSuffix) {
				key = key[:s.config.MaxAttributeKeyLength-len(TruncationSuffix)] + TruncationSuffix
			} else {
				key = TruncationSuffix[:s.config.MaxAttributeKeyLength]
			}
		}

		// Truncate value if too long and size limiting is enabled
		if s.config.EnableAttributeSizeLimiting && len(value) > s.config.MaxAttributeValueLength {
			if s.config.MaxAttributeValueLength > len(TruncationSuffix) {
				value = value[:s.config.MaxAttributeValueLength-len(TruncationSuffix)] + TruncationSuffix
			} else {
				value = TruncationSuffix[:s.config.MaxAttributeValueLength]
			}
		}

		sanitized = append(sanitized, attribute.String(key, value))
	}

	return sanitized
}

// sanitizeAttributeValue applies content-specific sanitization based on attribute key
func (s *DataSanitizer) sanitizeAttributeValue(key, value string) string {
	lowerKey := strings.ToLower(key)

	// Sanitize SQL statements
	if strings.Contains(lowerKey, "statement") || strings.Contains(lowerKey, "query") || strings.Contains(lowerKey, "sql") {
		return s.SanitizeSQL(value)
	}

	// Sanitize URLs
	if strings.Contains(lowerKey, "url") || strings.Contains(lowerKey, "endpoint") || strings.Contains(lowerKey, "uri") {
		return s.SanitizeURL(value)
	}

	// Redact sensitive attributes entirely
	if s.isSensitiveHeader(key) {
		return "[REDACTED]"
	}

	return value
}

// SanitizeError removes sensitive information from error messages
func (s *DataSanitizer) SanitizeError(err error) error {
	if err == nil {
		return nil
	}

	errMsg := err.Error()

	// Sanitize URLs in error messages
	if s.config.EnableURLSanitization {
		errMsg = s.sanitizeURLString(errMsg)
	}

	// Remove potential SQL literals from error messages
	if s.config.EnableSQLSanitization {
		errMsg = sqlStringLiteralRegex.ReplaceAllString(errMsg, "'[REDACTED]'")
	}

	return fmt.Errorf("%s", errMsg)
}

// CreateSanitizedAttribute creates a sanitized attribute key-value pair
func (s *DataSanitizer) CreateSanitizedAttribute(key, value string) attribute.KeyValue {
	attrs := s.SanitizeAttributes([]attribute.KeyValue{
		attribute.String(key, value),
	})

	if len(attrs) > 0 {
		return attrs[0]
	}

	return attribute.String(key, value)
}

// ValidateAttributeKey checks if an attribute key is valid and not too long
func (s *DataSanitizer) ValidateAttributeKey(key string) error {
	if key == "" {
		return fmt.Errorf("attribute key cannot be empty")
	}

	if len(key) > s.config.MaxAttributeKeyLength {
		return fmt.Errorf("attribute key too long: %d > %d", len(key), s.config.MaxAttributeKeyLength)
	}

	return nil
}

// ValidateAttributeValue checks if an attribute value is valid and not too long
func (s *DataSanitizer) ValidateAttributeValue(value string) error {
	if len(value) > s.config.MaxAttributeValueLength {
		return fmt.Errorf("attribute value too long: %d > %d", len(value), s.config.MaxAttributeValueLength)
	}

	return nil
}
