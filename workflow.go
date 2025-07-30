package treasuredata

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// WorkflowHook represents a single hook configuration
type WorkflowHook struct {
	Name        string   `json:"name"`
	Command     []string `json:"command"`
	Timeout     int      `json:"timeout,omitempty"`     // timeout in seconds, default 60
	FailOnError bool     `json:"fail_on_error"`         // whether to fail upload if hook fails
	WorkingDir  string   `json:"working_dir,omitempty"` // working directory, default is project directory
}

// WorkflowHooksConfig represents the hooks configuration file
type WorkflowHooksConfig struct {
	PreUploadHooks []WorkflowHook `json:"pre_upload_hooks"`
}

// Hook execution constants
const (
	DefaultHookTimeout = 60 * time.Second
	MaxHookTimeout     = 600 * time.Second // 10 minutes max
	MaxCommandLength   = 1000              // Maximum command string length
)

// validateHookCommand validates a hook command for security
func validateHookCommand(command []string) error {
	if len(command) == 0 {
		return fmt.Errorf("command cannot be empty")
	}

	// Validate each command argument
	for i, arg := range command {
		if len(arg) > MaxCommandLength {
			return fmt.Errorf("command argument %d too long (max %d characters)", i, MaxCommandLength)
		}
		
		// Block dangerous characters that could be used for command injection
		if strings.ContainsAny(arg, ";|&$`\n\r") {
			return fmt.Errorf("command argument %d contains dangerous characters", i)
		}
	}

	// Validate executable path (first argument)
	executable := command[0]
	if strings.Contains(executable, "..") {
		return fmt.Errorf("executable path cannot contain '..' for security reasons")
	}

	return nil
}

// validateWorkingDir validates and cleans a working directory path
func validateWorkingDir(workingDir, projectDir string) (string, error) {
	if workingDir == "" {
		return projectDir, nil
	}

	// Convert to absolute path
	var absWorkingDir string
	if filepath.IsAbs(workingDir) {
		absWorkingDir = workingDir
	} else {
		absWorkingDir = filepath.Join(projectDir, workingDir)
	}

	// Clean the path to resolve any .., ., etc.
	absWorkingDir = filepath.Clean(absWorkingDir)

	// Get absolute path of project directory for comparison
	absProjectDir, err := filepath.Abs(projectDir)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute project directory: %w", err)
	}
	absProjectDir = filepath.Clean(absProjectDir)

	// Security check: ensure working directory is within or equal to project directory
	relPath, err := filepath.Rel(absProjectDir, absWorkingDir)
	if err != nil {
		return "", fmt.Errorf("failed to compute relative path: %w", err)
	}

	// Check if the relative path tries to escape the project directory
	if strings.HasPrefix(relPath, "..") {
		return "", fmt.Errorf("working directory cannot be outside project directory (attempted: %s)", workingDir)
	}

	return absWorkingDir, nil
}

// validateHook validates a single hook configuration
func validateHook(hook WorkflowHook, projectDir string) error {
	if hook.Name == "" {
		return fmt.Errorf("hook name cannot be empty")
	}

	if err := validateHookCommand(hook.Command); err != nil {
		return fmt.Errorf("invalid command for hook '%s': %w", hook.Name, err)
	}

	// Validate timeout
	if hook.Timeout < 0 {
		return fmt.Errorf("hook '%s' timeout cannot be negative", hook.Name)
	}
	timeout := time.Duration(hook.Timeout) * time.Second
	if timeout > MaxHookTimeout {
		return fmt.Errorf("hook '%s' timeout %v exceeds maximum %v", hook.Name, timeout, MaxHookTimeout)
	}

	// Validate working directory
	if _, err := validateWorkingDir(hook.WorkingDir, projectDir); err != nil {
		return fmt.Errorf("invalid working directory for hook '%s': %w", hook.Name, err)
	}

	return nil
}

// loadHooksConfig loads hooks configuration from a directory
func loadHooksConfig(dirPath string) (*WorkflowHooksConfig, error) {
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// No hooks config found, return empty config
		return &WorkflowHooksConfig{}, nil
	}

	// Read and parse config file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read hooks config file %s: %w", configPath, err)
	}

	var config WorkflowHooksConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to parse hooks config file %s: %w", configPath, err)
	}

	// Validate all hooks in the configuration
	for _, hook := range config.PreUploadHooks {
		if err := validateHook(hook, dirPath); err != nil {
			return nil, fmt.Errorf("hook validation failed: %w", err)
		}
	}

	return &config, nil
}

// executeHook executes a single hook with security validations
func executeHook(hook WorkflowHook, projectDir string) error {
	// Validate hook configuration before execution
	if err := validateHook(hook, projectDir); err != nil {
		return err
	}

	// Set timeout (default 60 seconds)
	timeout := time.Duration(hook.Timeout) * time.Second
	if timeout == 0 {
		timeout = DefaultHookTimeout
	}

	// Validate and set working directory
	workingDir, err := validateWorkingDir(hook.WorkingDir, projectDir)
	if err != nil {
		return fmt.Errorf("hook '%s': %w", hook.Name, err)
	}

	// Create command with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Use validated command (already validated in validateHook)
	cmd := exec.CommandContext(ctx, hook.Command[0], hook.Command[1:]...)
	cmd.Dir = workingDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("Running hook '%s': %s\n", hook.Name, strings.Join(hook.Command, " "))
	fmt.Printf("Working directory: %s\n", workingDir)

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("hook '%s' timed out after %v", hook.Name, timeout)
		}
		return fmt.Errorf("hook '%s' failed: %w", hook.Name, err)
	}

	fmt.Printf("Hook '%s' completed successfully\n", hook.Name)
	return nil
}

// executePreUploadHooks executes all pre-upload hooks
func executePreUploadHooks(dirPath string) error {
	config, err := loadHooksConfig(dirPath)
	if err != nil {
		return err
	}

	if len(config.PreUploadHooks) == 0 {
		return nil // No hooks to execute
	}

	fmt.Printf("Executing %d pre-upload hook(s)...\n", len(config.PreUploadHooks))

	for _, hook := range config.PreUploadHooks {
		if err := executeHook(hook, dirPath); err != nil {
			if hook.FailOnError {
				return fmt.Errorf("pre-upload hook failed: %w", err)
			}
			fmt.Printf("Warning: Hook '%s' failed but continuing: %v\n", hook.Name, err)
		}
	}

	fmt.Println("All pre-upload hooks completed")
	return nil
}

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

// WorkflowProjectRef represents a workflow project reference
type WorkflowProjectRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Workflow represents a Treasure Data workflow
type Workflow struct {
	ID           string                 `json:"id"`
	Name         string                 `json:"name"`
	Project      WorkflowProjectRef     `json:"project"`
	Revision     string                 `json:"revision"`
	Status       string                 `json:"status"`
	Config       map[string]interface{} `json:"config"`
	CreatedAt    TDTime                 `json:"created_at"`
	UpdatedAt    TDTime                 `json:"updated_at"`
	LastAttempt  *int                   `json:"last_attempt"`
	NextSchedule *TDTime                `json:"next_schedule"`
	Timezone     string                 `json:"timezone"`
}

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

// WorkflowSchedule represents a workflow schedule
type WorkflowSchedule struct {
	ID               string  `json:"id"`
	WorkflowID       string  `json:"workflow_id"`
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

// WorkflowProject represents a workflow project
type WorkflowProject struct {
	ID          string  `json:"id"`
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
	u := "api/workflows"
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
func (s *WorkflowService) GetWorkflow(ctx context.Context, workflowID string) (*Workflow, error) {
	// Validate input
	if workflowID == "" {
		return nil, NewValidationError("workflowID", workflowID, "cannot be empty")
	}

	u := fmt.Sprintf("api/workflows/%s", workflowID)

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

// GetWorkflowSchedule retrieves the schedule for a workflow
func (s *WorkflowService) GetWorkflowSchedule(ctx context.Context, workflowID string) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("api/workflows/%s/schedule", workflowID)

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
func (s *WorkflowService) EnableWorkflowSchedule(ctx context.Context, workflowID string) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("api/workflows/%s/schedule/enable", workflowID)

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
func (s *WorkflowService) DisableWorkflowSchedule(ctx context.Context, workflowID string) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("api/workflows/%s/schedule/disable", workflowID)

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
func (s *WorkflowService) UpdateWorkflowSchedule(ctx context.Context, workflowID string, cron, timezone string, delay int) (*WorkflowSchedule, error) {
	// Validate input
	if workflowID == "" {
		return nil, NewValidationError("workflowID", workflowID, "cannot be empty")
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

	u := fmt.Sprintf("api/workflows/%s/schedule", workflowID)

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
func (s *WorkflowService) UpdateWorkflow(ctx context.Context, workflowID string, updates map[string]string) (*Workflow, error) {
	u := fmt.Sprintf("api/workflows/%s", workflowID)

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
func (s *WorkflowService) DeleteWorkflow(ctx context.Context, workflowID string) error {
	// Validate input
	if workflowID == "" {
		return NewValidationError("workflowID", workflowID, "cannot be empty")
	}

	u := fmt.Sprintf("api/workflows/%s", workflowID)

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
	u := "api/projects"

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
func (s *WorkflowService) GetProject(ctx context.Context, projectID string) (*WorkflowProject, error) {
	u := fmt.Sprintf("api/projects/%s", projectID)

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

// CreateProject creates a new workflow project with auto-generated revision based on content hash
func (s *WorkflowService) CreateProject(ctx context.Context, name string, archive []byte) (*WorkflowProject, error) {
	// Generate MD5 hash of the archive content as revision
	hash := md5.Sum(archive)
	revision := hex.EncodeToString(hash[:])

	return s.CreateProjectWithRevision(ctx, name, revision, archive)
}

// CreateProjectWithRevision creates a new workflow project with a specific revision
func (s *WorkflowService) CreateProjectWithRevision(ctx context.Context, name, revision string, archive []byte) (*WorkflowProject, error) {
	// If revision is empty, generate it from content hash
	if revision == "" {
		hash := md5.Sum(archive)
		revision = hex.EncodeToString(hash[:])
	}

	u := fmt.Sprintf("api/projects?project=%s&revision=%s", name, revision)

	// Use binary request with appropriate content type for tar.gz archives
	req, err := s.client.NewWorkflowBinaryRequest("PUT", u, archive, "application/gzip")
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

// CreateProjectFromDirectory creates a new workflow project from a directory with auto-generated revision
func (s *WorkflowService) CreateProjectFromDirectory(ctx context.Context, name string, dirPath string) (*WorkflowProject, error) {
	// Execute pre-upload hooks
	if err := executePreUploadHooks(dirPath); err != nil {
		return nil, fmt.Errorf("pre-upload hooks failed: %w", err)
	}

	// Create tar.gz archive from directory
	archive, err := createTarGz(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive from directory %s: %w", dirPath, err)
	}

	// Generate MD5 hash of the archive content as revision
	hash := md5.Sum(archive)
	revision := hex.EncodeToString(hash[:])

	return s.CreateProjectWithRevision(ctx, name, revision, archive)
}

// CreateProjectFromDirectoryWithRevision creates a new workflow project from a directory with a specific revision
func (s *WorkflowService) CreateProjectFromDirectoryWithRevision(ctx context.Context, name, revision, dirPath string) (*WorkflowProject, error) {
	// Execute pre-upload hooks
	if err := executePreUploadHooks(dirPath); err != nil {
		return nil, fmt.Errorf("pre-upload hooks failed: %w", err)
	}

	// Create tar.gz archive from directory
	archive, err := createTarGz(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive from directory %s: %w", dirPath, err)
	}

	// If revision is empty, it will be auto-generated in CreateProjectWithRevision
	return s.CreateProjectWithRevision(ctx, name, revision, archive)
}

// ListProjectWorkflows returns a list of workflows for a specific project
func (s *WorkflowService) ListProjectWorkflows(ctx context.Context, projectID string) (*WorkflowListResponse, error) {
	u := fmt.Sprintf("api/projects/%s/workflows", projectID)

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
func (s *WorkflowService) GetProjectSecrets(ctx context.Context, projectID string) (*WorkflowProjectSecretsResponse, error) {
	u := fmt.Sprintf("api/projects/%s/secrets", projectID)

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
func (s *WorkflowService) SetProjectSecret(ctx context.Context, projectID string, key, value string) error {
	// Validate input
	if projectID == "" {
		return NewValidationError("projectID", projectID, "cannot be empty")
	}
	if key == "" {
		return NewValidationError("key", key, "cannot be empty")
	}

	u := fmt.Sprintf("api/projects/%s/secrets/%s", projectID, key)

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
func (s *WorkflowService) DeleteProjectSecret(ctx context.Context, projectID string, key string) error {
	u := fmt.Sprintf("api/projects/%s/secrets/%s", projectID, key)

	req, err := s.client.NewWorkflowRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete project secret: project_id=%s, key=%s", projectID, key)
	}

	return nil
}
