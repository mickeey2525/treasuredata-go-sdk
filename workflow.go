package treasuredata

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// validateCronExpression validates a cron expression
func validateCronExpression(cron string) error {
	// Basic cron expression validation
	// Standard cron format: minute hour day month weekday
	// Extended format with seconds: second minute hour day month weekday
	// Special strings: @yearly, @annually, @monthly, @weekly, @daily, @midnight, @hourly

	// Check for special strings first
	specialStrings := []string{"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"}
	for _, special := range specialStrings {
		if cron == special {
			return nil
		}
	}

	// Split cron expression into fields
	fields := strings.Fields(cron)
	if len(fields) < 5 || len(fields) > 6 {
		return fmt.Errorf("cron expression must have 5 or 6 fields, got %d", len(fields))
	}

	// Basic validation for each field
	for i, field := range fields {
		if field == "" {
			return fmt.Errorf("field %d cannot be empty", i+1)
		}
		// Allow common cron characters
		if !regexp.MustCompile(`^[\*0-9,\-\/]+$`).MatchString(field) {
			return fmt.Errorf("field %d contains invalid characters: %s", i+1, field)
		}
	}

	return nil
}

// createTarGz creates a tar.gz archive from a directory
func createTarGz(sourceDir string) ([]byte, error) {
	// Define reasonable limits
	const (
		maxFileSize  = 100 * 1024 * 1024 // 100MB per file
		maxTotalSize = 500 * 1024 * 1024 // 500MB total archive size
		maxFiles     = 10000             // Maximum number of files
	)

	var (
		buf       bytes.Buffer
		totalSize int64
		fileCount int
	)

	// Ensure sourceDir is absolute
	absSourceDir, err := filepath.Abs(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	err = filepath.Walk(absSourceDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Security check: reject symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinks not allowed: %s", filePath)
		}

		// Get relative path
		relPath, err := filepath.Rel(absSourceDir, filePath)
		if err != nil {
			return err
		}

		// Security check: ensure path doesn't escape source directory
		if strings.HasPrefix(relPath, "..") || filepath.IsAbs(relPath) {
			return fmt.Errorf("path traversal detected: %s", relPath)
		}

		// Skip hidden files and directories (starting with .)
		if strings.HasPrefix(filepath.Base(filePath), ".") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Check file count limit
		fileCount++
		if fileCount > maxFiles {
			return fmt.Errorf("too many files: maximum %d files allowed", maxFiles)
		}

		// Check file size limit
		if info.Mode().IsRegular() && info.Size() > maxFileSize {
			return fmt.Errorf("file too large: %s (size: %d bytes, max: %d bytes)", filePath, info.Size(), maxFileSize)
		}

		// Check total size limit
		totalSize += info.Size()
		if totalSize > maxTotalSize {
			return fmt.Errorf("archive too large: total size %d bytes exceeds maximum %d bytes", totalSize, maxTotalSize)
		}

		// Create tar header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		// Use relative path as name
		header.Name = relPath

		// Write header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// If it's a regular file, write the content
		if info.Mode().IsRegular() {
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}

			// Ensure file is closed even if io.Copy fails
			_, copyErr := io.Copy(tw, file)
			closeErr := file.Close()

			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Close tar writer
	if err := tw.Close(); err != nil {
		return nil, err
	}

	// Close gzip writer
	if err := gw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

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

// WorkflowProject represents a workflow project
type WorkflowProject struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Revision    string  `json:"revision"`
	ArchiveType string  `json:"archiveType"`
	ArchiveMD5  string  `json:"archiveMd5"`
	CreatedAt   TDTime  `json:"createdAt"`
	UpdatedAt   TDTime  `json:"updatedAt"`
	DeletedAt   *TDTime `json:"deletedAt"`
}

// WorkflowProjectSecret represents a project secret
type WorkflowProjectSecret struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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

// WorkflowProjectListResponse represents the response from the workflow project list API
type WorkflowProjectListResponse struct {
	Projects []WorkflowProject `json:"projects"`
}

// WorkflowProjectSecretsResponse represents the response from the workflow project secrets API
type WorkflowProjectSecretsResponse struct {
	Secrets map[string]string `json:"secrets"`
}

// ListWorkflows returns a list of workflows
func (s *WorkflowService) ListWorkflows(ctx context.Context, opts *WorkflowListOptions) (*WorkflowListResponse, error) {
	u := fmt.Sprintf("api/workflows")
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
	// Validate input
	if workflowID <= 0 {
		return nil, NewValidationError("workflowID", workflowID, "must be a positive integer")
	}

	u := fmt.Sprintf("api/workflows/%d", workflowID)

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
	// Validate input
	if workflowID <= 0 {
		return nil, NewValidationError("workflowID", workflowID, "must be a positive integer")
	}

	u := fmt.Sprintf("api/workflows/%d/attempts", workflowID)

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
	u := fmt.Sprintf("api/workflows/%d/attempts", workflowID)
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
	u := fmt.Sprintf("api/workflows/%d/attempts/%d", workflowID, attemptID)

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
	// Validate input
	if workflowID <= 0 {
		return NewValidationError("workflowID", workflowID, "must be a positive integer")
	}
	if attemptID <= 0 {
		return NewValidationError("attemptID", attemptID, "must be a positive integer")
	}

	u := fmt.Sprintf("api/workflows/%d/attempts/%d/kill", workflowID, attemptID)

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
func (s *WorkflowService) RetryWorkflowAttempt(ctx context.Context, workflowID int, attemptID int, params map[string]interface{}) (*WorkflowAttempt, error) {
	u := fmt.Sprintf("api/workflows/%d/attempts/%d/retry", workflowID, attemptID)

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
	u := fmt.Sprintf("api/workflows/%d/attempts/%d/tasks", workflowID, attemptID)

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
	u := fmt.Sprintf("api/workflows/%d/attempts/%d/tasks/%s", workflowID, attemptID, taskID)

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
	u := fmt.Sprintf("api/workflows/%d/attempts/%d/log", workflowID, attemptID)

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
func (s *WorkflowService) GetWorkflowTaskLog(ctx context.Context, workflowID int, attemptID int, taskID string) (string, error) {
	u := fmt.Sprintf("api/workflows/%d/attempts/%d/tasks/%s/log", workflowID, attemptID, taskID)

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

// GetWorkflowSchedule retrieves the schedule for a workflow
func (s *WorkflowService) GetWorkflowSchedule(ctx context.Context, workflowID int) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("api/workflows/%d/schedule", workflowID)

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
	u := fmt.Sprintf("api/workflows/%d/schedule/enable", workflowID)

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
	u := fmt.Sprintf("api/workflows/%d/schedule/disable", workflowID)

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
	// Validate input
	if workflowID <= 0 {
		return nil, NewValidationError("workflowID", workflowID, "must be a positive integer")
	}
	if cron == "" {
		return nil, NewValidationError("cron", cron, "cannot be empty")
	}
	if err := validateCronExpression(cron); err != nil {
		return nil, NewValidationError("cron", cron, err.Error())
	}
	if timezone == "" {
		return nil, NewValidationError("timezone", timezone, "cannot be empty")
	}
	if delay < 0 {
		return nil, NewValidationError("delay", delay, "cannot be negative")
	}

	u := fmt.Sprintf("api/workflows/%d/schedule", workflowID)

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
	// Validate input
	if name == "" {
		return nil, NewValidationError("name", name, "cannot be empty")
	}
	if project == "" {
		return nil, NewValidationError("project", project, "cannot be empty")
	}
	if config == "" {
		return nil, NewValidationError("config", config, "cannot be empty")
	}

	u := "api/workflows"

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
	u := fmt.Sprintf("api/workflows/%d", workflowID)

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
	// Validate input
	if workflowID <= 0 {
		return NewValidationError("workflowID", workflowID, "must be a positive integer")
	}

	u := fmt.Sprintf("api/workflows/%d", workflowID)

	req, err := s.client.NewWorkflowRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return &WorkflowError{
			Operation:  "delete workflow",
			WorkflowID: workflowID,
			StatusCode: resp.StatusCode,
			Response:   resp,
		}
	}

	return nil
}

// ListProjects returns a list of workflow projects
func (s *WorkflowService) ListProjects(ctx context.Context) (*WorkflowProjectListResponse, error) {
	u := fmt.Sprintf("api/projects")

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp WorkflowProjectListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetProject retrieves a specific project by ID
func (s *WorkflowService) GetProject(ctx context.Context, projectID int) (*WorkflowProject, error) {
	u := fmt.Sprintf("api/projects/%d", projectID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var project WorkflowProject
	_, err = s.client.Do(ctx, req, &project)
	if err != nil {
		return nil, err
	}

	return &project, nil
}

// CreateProject creates a new workflow project
func (s *WorkflowService) CreateProject(ctx context.Context, name string, archive []byte) (*WorkflowProject, error) {
	u := fmt.Sprintf("api/projects?project=%s", name)

	req, err := s.client.NewWorkflowRequest("PUT", u, archive)
	if err != nil {
		return nil, err
	}

	var project WorkflowProject
	_, err = s.client.Do(ctx, req, &project)
	if err != nil {
		return nil, err
	}

	return &project, nil
}

// CreateProjectFromDirectory creates a new workflow project from a directory
func (s *WorkflowService) CreateProjectFromDirectory(ctx context.Context, name string, dirPath string) (*WorkflowProject, error) {
	// Create tar.gz archive from directory
	archive, err := createTarGz(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive from directory %s: %w", dirPath, err)
	}

	return s.CreateProject(ctx, name, archive)
}

// ListProjectWorkflows returns a list of workflows for a specific project
func (s *WorkflowService) ListProjectWorkflows(ctx context.Context, projectID int) (*WorkflowListResponse, error) {
	u := fmt.Sprintf("api/projects/%d/workflows", projectID)

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

// GetProjectSecrets retrieves secrets for a project
func (s *WorkflowService) GetProjectSecrets(ctx context.Context, projectID int) (*WorkflowProjectSecretsResponse, error) {
	u := fmt.Sprintf("api/projects/%d/secrets", projectID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp WorkflowProjectSecretsResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// SetProjectSecret sets a secret for a project
func (s *WorkflowService) SetProjectSecret(ctx context.Context, projectID int, key, value string) error {
	// Validate input
	if projectID <= 0 {
		return NewValidationError("projectID", projectID, "must be a positive integer")
	}
	if key == "" {
		return NewValidationError("key", key, "cannot be empty")
	}

	u := fmt.Sprintf("api/projects/%d/secrets/%s", projectID, key)

	body := map[string]string{
		"value": value,
	}

	req, err := s.client.NewWorkflowRequest("PUT", u, body)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return &WorkflowError{
			Operation:  "set project secret",
			ProjectID:  projectID,
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("key=%s", key),
			Response:   resp,
		}
	}

	return nil
}

// DeleteProjectSecret deletes a secret from a project
func (s *WorkflowService) DeleteProjectSecret(ctx context.Context, projectID int, key string) error {
	u := fmt.Sprintf("api/projects/%d/secrets/%s", projectID, key)

	req, err := s.client.NewWorkflowRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete project secret: project_id=%d, key=%s", projectID, key)
	}

	return nil
}
