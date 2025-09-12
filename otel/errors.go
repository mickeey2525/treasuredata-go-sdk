package otel

import "fmt"

// ErrorSeverity represents the severity level of an OTEL error
type ErrorSeverity int

const (
	SeverityWarning ErrorSeverity = iota
	SeverityError
	SeverityCritical
)

// String returns the string representation of the error severity
func (s ErrorSeverity) String() string {
	switch s {
	case SeverityWarning:
		return "WARNING"
	case SeverityError:
		return "ERROR"
	case SeverityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// OTELError wraps errors with additional context and severity information
type OTELError struct {
	Operation string
	Cause     error
	Severity  ErrorSeverity
}

// Error implements the error interface
func (e *OTELError) Error() string {
	return fmt.Sprintf("[%s] OTEL %s: %v", e.Severity, e.Operation, e.Cause)
}

// Unwrap returns the underlying error
func (e *OTELError) Unwrap() error {
	return e.Cause
}

// NewOTELError creates a new OTEL error with the specified severity
func NewOTELError(operation string, cause error, severity ErrorSeverity) *OTELError {
	return &OTELError{
		Operation: operation,
		Cause:     cause,
		Severity:  severity,
	}
}

// NewWarning creates a new warning-level OTEL error
func NewWarning(operation string, cause error) *OTELError {
	return NewOTELError(operation, cause, SeverityWarning)
}

// NewError creates a new error-level OTEL error
func NewError(operation string, cause error) *OTELError {
	return NewOTELError(operation, cause, SeverityError)
}

// NewCritical creates a new critical-level OTEL error
func NewCritical(operation string, cause error) *OTELError {
	return NewOTELError(operation, cause, SeverityCritical)
}
