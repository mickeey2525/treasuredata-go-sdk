package treasuredata

import (
	"fmt"
	"net/http"
)

// WorkflowError represents an error that occurred during workflow operations
type WorkflowError struct {
	Operation  string
	WorkflowID int
	AttemptID  int
	TaskID     string
	ProjectID  int
	StatusCode int
	Message    string
	Response   *http.Response
}

// Error returns the error message
func (e *WorkflowError) Error() string {
	msg := fmt.Sprintf("workflow error: %s", e.Operation)

	if e.WorkflowID > 0 {
		msg += fmt.Sprintf(" (workflow_id=%d", e.WorkflowID)
		if e.AttemptID > 0 {
			msg += fmt.Sprintf(", attempt_id=%d", e.AttemptID)
		}
		if e.TaskID != "" {
			msg += fmt.Sprintf(", task_id=%s", e.TaskID)
		}
		msg += ")"
	} else if e.ProjectID > 0 {
		msg += fmt.Sprintf(" (project_id=%d", e.ProjectID)
		msg += ")"
	}

	if e.StatusCode > 0 {
		msg += fmt.Sprintf(" - HTTP %d", e.StatusCode)
	}

	if e.Message != "" {
		msg += fmt.Sprintf(": %s", e.Message)
	}

	return msg
}

// ValidationError represents an error when input validation fails
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

// Error returns the error message
func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("validation error: %s - %s", e.Field, e.Message)
	}
	return fmt.Sprintf("validation error: %s", e.Message)
}

// NewWorkflowError creates a new WorkflowError
func NewWorkflowError(operation string, statusCode int, message string) *WorkflowError {
	return &WorkflowError{
		Operation:  operation,
		StatusCode: statusCode,
		Message:    message,
	}
}

// NewValidationError creates a new ValidationError
func NewValidationError(field string, value interface{}, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}
