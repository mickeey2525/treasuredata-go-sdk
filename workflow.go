package treasuredata

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
)

// WorkflowService handles communication with the Workflow related methods of the Treasure Data API.
type WorkflowService struct {
	client *Client
}

// Workflow represents a Treasure Data workflow
type Workflow struct {
	ID           int     `json:"id"`
	Name         string  `json:"name"`
	Project      string  `json:"project"`
	Revision     string  `json:"revision"`
	Status       string  `json:"status"`
	Config       string  `json:"config"`
	CreatedAt    TDTime  `json:"created_at"`
	UpdatedAt    TDTime  `json:"updated_at"`
	LastAttempt  *int    `json:"last_attempt"`
	NextSchedule *TDTime `json:"next_schedule"`
	Timezone     string  `json:"timezone"`
}

// WorkflowAttempt represents a workflow execution attempt
type WorkflowAttempt struct {
	ID          int                    `json:"id"`
	Index       int                    `json:"index"`
	WorkflowID  int                    `json:"workflow_id"`
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

// WorkflowSchedule represents a workflow schedule
type WorkflowSchedule struct {
	ID               int     `json:"id"`
	WorkflowID       int     `json:"workflow_id"`
	Cron             string  `json:"cron"`
	Timezone         string  `json:"timezone"`
	Delay            int     `json:"delay"`
	NextTime         *TDTime `json:"next_time"`
	NextScheduleTime *TDTime `json:"next_schedule_time"`
	DisabledAt       *TDTime `json:"disabled_at"`
	CreatedAt        TDTime  `json:"created_at"`
	UpdatedAt        TDTime  `json:"updated_at"`
}

// WorkflowSession represents a workflow session
type WorkflowSession struct {
	ID          string                 `json:"id"`
	WorkflowID  int                    `json:"workflow_id"`
	AttemptID   int                    `json:"attempt_id"`
	SessionID   string                 `json:"session_id"`
	SessionUUID string                 `json:"session_uuid"`
	SessionTime TDTime                 `json:"session_time"`
	Status      string                 `json:"status"`
	LastAttempt int                    `json:"last_attempt"`
	Params      map[string]interface{} `json:"params"`
	CreatedAt   TDTime                 `json:"created_at"`
	UpdatedAt   TDTime                 `json:"updated_at"`
}

// WorkflowListOptions specifies optional parameters to Workflow List method
type WorkflowListOptions struct {
	Limit  int `url:"limit,omitempty"`
	Offset int `url:"offset,omitempty"`
}

// WorkflowAttemptListOptions specifies optional parameters to WorkflowAttempt List method
type WorkflowAttemptListOptions struct {
	Limit  int    `url:"limit,omitempty"`
	Offset int    `url:"offset,omitempty"`
	LastID int    `url:"last_id,omitempty"`
	Status string `url:"status,omitempty"`
}

// WorkflowListResponse represents the response from the workflow list API
type WorkflowListResponse struct {
	Workflows []Workflow `json:"workflows"`
}

// WorkflowAttemptListResponse represents the response from the workflow attempt list API
type WorkflowAttemptListResponse struct {
	Attempts []WorkflowAttempt `json:"attempts"`
}

// WorkflowTaskListResponse represents the response from the workflow task list API
type WorkflowTaskListResponse struct {
	Tasks []WorkflowTask `json:"tasks"`
}

// ListWorkflows returns a list of workflows
func (s *WorkflowService) ListWorkflows(ctx context.Context, opts *WorkflowListOptions) (*WorkflowListResponse, error) {
	u := fmt.Sprintf("workflows")
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp WorkflowListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetWorkflow retrieves a specific workflow by ID
func (s *WorkflowService) GetWorkflow(ctx context.Context, workflowID int) (*Workflow, error) {
	u := fmt.Sprintf("workflows/%d", workflowID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var workflow Workflow
	_, err = s.client.Do(ctx, req, &workflow)
	if err != nil {
		return nil, err
	}

	return &workflow, nil
}

// StartWorkflow starts a workflow manually
func (s *WorkflowService) StartWorkflow(ctx context.Context, workflowID int, params map[string]interface{}) (*WorkflowAttempt, error) {
	u := fmt.Sprintf("workflows/%d/attempts", workflowID)

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
func (s *WorkflowService) ListWorkflowAttempts(ctx context.Context, workflowID int, opts *WorkflowAttemptListOptions) (*WorkflowAttemptListResponse, error) {
	u := fmt.Sprintf("workflows/%d/attempts", workflowID)
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
func (s *WorkflowService) GetWorkflowAttempt(ctx context.Context, workflowID int, attemptID int) (*WorkflowAttempt, error) {
	u := fmt.Sprintf("workflows/%d/attempts/%d", workflowID, attemptID)

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
func (s *WorkflowService) KillWorkflowAttempt(ctx context.Context, workflowID int, attemptID int) error {
	u := fmt.Sprintf("workflows/%d/attempts/%d/kill", workflowID, attemptID)

	req, err := s.client.NewWorkflowRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to kill workflow attempt: workflow_id=%d, attempt_id=%d", workflowID, attemptID)
	}

	return nil
}

// RetryWorkflowAttempt retries a failed workflow attempt
func (s *WorkflowService) RetryWorkflowAttempt(ctx context.Context, workflowID int, attemptID int, params map[string]interface{}) (*WorkflowAttempt, error) {
	u := fmt.Sprintf("workflows/%d/attempts/%d/retry", workflowID, attemptID)

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
func (s *WorkflowService) ListWorkflowTasks(ctx context.Context, workflowID int, attemptID int) (*WorkflowTaskListResponse, error) {
	u := fmt.Sprintf("workflows/%d/attempts/%d/tasks", workflowID, attemptID)

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
func (s *WorkflowService) GetWorkflowTask(ctx context.Context, workflowID int, attemptID int, taskID string) (*WorkflowTask, error) {
	u := fmt.Sprintf("workflows/%d/attempts/%d/tasks/%s", workflowID, attemptID, taskID)

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
func (s *WorkflowService) GetWorkflowAttemptLog(ctx context.Context, workflowID int, attemptID int) (string, error) {
	u := fmt.Sprintf("workflows/%d/attempts/%d/log", workflowID, attemptID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return "", err
	}

	var logContent string
	resp, err := s.client.Do(ctx, req, &logContent)
	if err != nil {
		return "", err
	}

	// If response is plain text, read directly
	if resp.Header.Get("Content-Type") == "text/plain" {
		defer resp.Body.Close()
		buf := make([]byte, resp.ContentLength)
		_, err := resp.Body.Read(buf)
		if err != nil {
			return "", err
		}
		return string(buf), nil
	}

	return logContent, nil
}

// GetWorkflowTaskLog retrieves the log for a specific workflow task
func (s *WorkflowService) GetWorkflowTaskLog(ctx context.Context, workflowID int, attemptID int, taskID string) (string, error) {
	u := fmt.Sprintf("workflows/%d/attempts/%d/tasks/%s/log", workflowID, attemptID, taskID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return "", err
	}

	var logContent string
	resp, err := s.client.Do(ctx, req, &logContent)
	if err != nil {
		return "", err
	}

	// If response is plain text, read directly
	if resp.Header.Get("Content-Type") == "text/plain" {
		defer resp.Body.Close()
		buf := make([]byte, resp.ContentLength)
		_, err := resp.Body.Read(buf)
		if err != nil {
			return "", err
		}
		return string(buf), nil
	}

	return logContent, nil
}

// GetWorkflowSchedule retrieves the schedule for a workflow
func (s *WorkflowService) GetWorkflowSchedule(ctx context.Context, workflowID int) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("workflows/%d/schedule", workflowID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var schedule WorkflowSchedule
	_, err = s.client.Do(ctx, req, &schedule)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

// EnableWorkflowSchedule enables the schedule for a workflow
func (s *WorkflowService) EnableWorkflowSchedule(ctx context.Context, workflowID int) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("workflows/%d/schedule/enable", workflowID)

	req, err := s.client.NewWorkflowRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var schedule WorkflowSchedule
	_, err = s.client.Do(ctx, req, &schedule)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

// DisableWorkflowSchedule disables the schedule for a workflow
func (s *WorkflowService) DisableWorkflowSchedule(ctx context.Context, workflowID int) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("workflows/%d/schedule/disable", workflowID)

	req, err := s.client.NewWorkflowRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var schedule WorkflowSchedule
	_, err = s.client.Do(ctx, req, &schedule)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

// UpdateWorkflowSchedule updates the schedule for a workflow
func (s *WorkflowService) UpdateWorkflowSchedule(ctx context.Context, workflowID int, cron, timezone string, delay int) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("workflows/%d/schedule", workflowID)

	body := map[string]interface{}{
		"cron":     cron,
		"timezone": timezone,
		"delay":    delay,
	}

	req, err := s.client.NewWorkflowRequest("PUT", u, body)
	if err != nil {
		return nil, err
	}

	var schedule WorkflowSchedule
	_, err = s.client.Do(ctx, req, &schedule)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

// CreateWorkflow creates a new workflow
func (s *WorkflowService) CreateWorkflow(ctx context.Context, name, project, config string) (*Workflow, error) {
	u := fmt.Sprintf("workflows")

	body := map[string]string{
		"name":    name,
		"project": project,
		"config":  config,
	}

	req, err := s.client.NewWorkflowRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var workflow Workflow
	_, err = s.client.Do(ctx, req, &workflow)
	if err != nil {
		return nil, err
	}

	return &workflow, nil
}

// UpdateWorkflow updates an existing workflow
func (s *WorkflowService) UpdateWorkflow(ctx context.Context, workflowID int, updates map[string]string) (*Workflow, error) {
	u := fmt.Sprintf("workflows/%d", workflowID)

	req, err := s.client.NewWorkflowRequest("PUT", u, updates)
	if err != nil {
		return nil, err
	}

	var workflow Workflow
	_, err = s.client.Do(ctx, req, &workflow)
	if err != nil {
		return nil, err
	}

	return &workflow, nil
}

// DeleteWorkflow deletes a workflow
func (s *WorkflowService) DeleteWorkflow(ctx context.Context, workflowID int) error {
	u := fmt.Sprintf("workflows/%d", workflowID)

	req, err := s.client.NewWorkflowRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete workflow: %s", strconv.Itoa(workflowID))
	}

	return nil
}
