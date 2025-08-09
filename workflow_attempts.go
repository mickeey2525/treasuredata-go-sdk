package treasuredata

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
)

// WorkflowAttempt represents a workflow execution attempt
type WorkflowAttempt struct {
	ID          string                 `json:"id"`
	Index       int                    `json:"index"`
	WorkflowID  string                 `json:"workflow_id"`
	Status      string                 `json:"status"`
	CreatedAt   TDTime                 `json:"created_at"`
	FinishedAt  *TDTime                `json:"finished_at"`
	SessionID   *string                `json:"session_id"`
	SessionUUID *string                `json:"session_uuid"`
	SessionTime *TDTime                `json:"session_time"`
	Params      map[string]interface{} `json:"params"`
	LogFileSize *int64                 `json:"log_file_size"`
	Success     *bool                  `json:"success"`
	Done        bool                   `json:"done"`
}

// WorkflowTask represents a task within a workflow attempt
type WorkflowTask struct {
	ID           string                  `json:"id"`
	FullName     string                  `json:"full_name"`
	ParentID     *string                 `json:"parent_id"`
	Config       map[string]interface{}  `json:"config"`
	UpstreamsID  []string                `json:"upstreams"`
	IsGroup      bool                    `json:"is_group"`
	State        string                  `json:"state"`
	ExportParams map[string]interface{}  `json:"export_params"`
	StoreParams  map[string]interface{}  `json:"store_params"`
	ReportID     *string                 `json:"report"`
	Error        *map[string]interface{} `json:"error"`
	RetryAt      *TDTime                 `json:"retry_at"`
	StartedAt    *TDTime                 `json:"started_at"`
	UpdatedAt    TDTime                  `json:"updated_at"`
}

// WorkflowSession represents a workflow session
type WorkflowSession struct {
	ID          string                 `json:"id"`
	WorkflowID  string                 `json:"workflow_id"`
	AttemptID   string                 `json:"attempt_id"`
	SessionID   string                 `json:"session_id"`
	SessionUUID string                 `json:"session_uuid"`
	SessionTime TDTime                 `json:"session_time"`
	Status      string                 `json:"status"`
	LastAttempt string                 `json:"last_attempt"`
	Params      map[string]interface{} `json:"params"`
	CreatedAt   TDTime                 `json:"created_at"`
	UpdatedAt   TDTime                 `json:"updated_at"`
}

// WorkflowAttemptListOptions specifies optional parameters to WorkflowAttempt List method
type WorkflowAttemptListOptions struct {
	Limit  int    `url:"limit,omitempty"`
	Offset int    `url:"offset,omitempty"`
	LastID int    `url:"last_id,omitempty"`
	Status string `url:"status,omitempty"`
}

// WorkflowAttemptListResponse represents the response from the workflow attempt list API
type WorkflowAttemptListResponse struct {
	Attempts []WorkflowAttempt `json:"attempts"`
}

// WorkflowTaskListResponse represents the response from the workflow task list API
type WorkflowTaskListResponse struct {
	Tasks []WorkflowTask `json:"tasks"`
}

// StartWorkflow starts a workflow manually
func (s *WorkflowService) StartWorkflow(ctx context.Context, workflowID string, params map[string]interface{}) (*WorkflowAttempt, error) {
	// Validate input
	if workflowID == "" {
		return nil, NewValidationError("workflowID", workflowID, "cannot be empty")
	}

	u := fmt.Sprintf("api/workflows/%s/attempts", workflowID)

	body := map[string]interface{}{}
	if params != nil {
		body["params"] = params
	}

	req, err := s.client.NewWorkflowRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var attempt WorkflowAttempt
	_, err = s.client.Do(ctx, req, &attempt)
	if err != nil {
		return nil, err
	}

	return &attempt, nil
}

// ListWorkflowAttempts returns a list of workflow attempts
func (s *WorkflowService) ListWorkflowAttempts(ctx context.Context, workflowID string, opts *WorkflowAttemptListOptions) (*WorkflowAttemptListResponse, error) {
	u := fmt.Sprintf("api/workflows/%s/attempts", workflowID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp WorkflowAttemptListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetWorkflowAttempt retrieves a specific workflow attempt
func (s *WorkflowService) GetWorkflowAttempt(ctx context.Context, workflowID string, attemptID string) (*WorkflowAttempt, error) {
	u := fmt.Sprintf("api/workflows/%s/attempts/%s", workflowID, attemptID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var attempt WorkflowAttempt
	_, err = s.client.Do(ctx, req, &attempt)
	if err != nil {
		return nil, err
	}

	return &attempt, nil
}

// KillWorkflowAttempt kills a running workflow attempt
func (s *WorkflowService) KillWorkflowAttempt(ctx context.Context, workflowID string, attemptID string) error {
	// Validate input
	if workflowID == "" {
		return NewValidationError("workflowID", workflowID, "cannot be empty")
	}
	if attemptID == "" {
		return NewValidationError("attemptID", attemptID, "cannot be empty")
	}

	u := fmt.Sprintf("api/workflows/%s/attempts/%s/kill", workflowID, attemptID)

	req, err := s.client.NewWorkflowRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return &WorkflowError{
			Operation:  "kill workflow attempt",
			WorkflowID: workflowID,
			AttemptID:  attemptID,
			StatusCode: resp.StatusCode,
			Response:   resp,
		}
	}

	return nil
}

// RetryWorkflowAttempt retries a failed workflow attempt
func (s *WorkflowService) RetryWorkflowAttempt(ctx context.Context, workflowID string, attemptID string, params map[string]interface{}) (*WorkflowAttempt, error) {
	u := fmt.Sprintf("api/workflows/%s/attempts/%s/retry", workflowID, attemptID)

	body := map[string]interface{}{}
	if params != nil {
		body["params"] = params
	}

	req, err := s.client.NewWorkflowRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var attempt WorkflowAttempt
	_, err = s.client.Do(ctx, req, &attempt)
	if err != nil {
		return nil, err
	}

	return &attempt, nil
}

// ListWorkflowTasks returns a list of tasks for a workflow attempt
func (s *WorkflowService) ListWorkflowTasks(ctx context.Context, workflowID string, attemptID string) (*WorkflowTaskListResponse, error) {
	u := fmt.Sprintf("api/workflows/%s/attempts/%s/tasks", workflowID, attemptID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp WorkflowTaskListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetWorkflowTask retrieves a specific workflow task
func (s *WorkflowService) GetWorkflowTask(ctx context.Context, workflowID string, attemptID string, taskID string) (*WorkflowTask, error) {
	u := fmt.Sprintf("api/workflows/%s/attempts/%s/tasks/%s", workflowID, attemptID, taskID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var task WorkflowTask
	_, err = s.client.Do(ctx, req, &task)
	if err != nil {
		return nil, err
	}

	return &task, nil
}

// GetWorkflowAttemptLog retrieves the log for a workflow attempt
func (s *WorkflowService) GetWorkflowAttemptLog(ctx context.Context, workflowID string, attemptID string) (string, error) {
	u := fmt.Sprintf("api/workflows/%s/attempts/%s/log", workflowID, attemptID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return "", err
	}

	// Use a bytes.Buffer to capture the response
	var buf bytes.Buffer
	_, err = s.client.Do(ctx, req, &buf)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

// GetWorkflowTaskLog retrieves the log for a specific workflow task
func (s *WorkflowService) GetWorkflowTaskLog(ctx context.Context, workflowID string, attemptID string, taskID string) (string, error) {
	u := fmt.Sprintf("api/workflows/%s/attempts/%s/tasks/%s/log", workflowID, attemptID, taskID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return "", err
	}

	// Use a bytes.Buffer to capture the response
	var buf bytes.Buffer
	_, err = s.client.Do(ctx, req, &buf)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
