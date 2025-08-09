package treasuredata

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
)

// WorkflowProjectRef represents a workflow project reference
type WorkflowProjectRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
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

// WorkflowProjectListResponse represents the response from the workflow project list API
type WorkflowProjectListResponse struct {
	Projects []WorkflowProject `json:"projects"`
}

// WorkflowProjectSecretsResponse represents the response from the workflow project secrets API
type WorkflowProjectSecretsResponse struct {
	Secrets map[string]string `json:"secrets"`
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

// DownloadProject downloads a project archive as raw bytes
func (s *WorkflowService) DownloadProject(ctx context.Context, projectID string) ([]byte, error) {
	return s.DownloadProjectWithRevision(ctx, projectID, "")
}

// DownloadProjectWithRevision downloads a specific revision of a project archive as raw bytes
func (s *WorkflowService) DownloadProjectWithRevision(ctx context.Context, projectID, revision string) ([]byte, error) {
	// Validate input
	if projectID == "" {
		return nil, NewValidationError("projectID", projectID, "cannot be empty")
	}

	u := fmt.Sprintf("api/projects/%s/archive", projectID)
	if revision != "" {
		u += fmt.Sprintf("?revision=%s", revision)
	}

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	// Override Accept header for binary download
	req.Header.Set("Accept", "application/gzip, application/x-gzip, application/octet-stream, */*")

	var buf bytes.Buffer
	resp, err := s.client.Do(ctx, req, &buf)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &WorkflowError{
			Operation:  "download project",
			ProjectID:  projectID,
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("revision=%s", revision),
			Response:   resp,
		}
	}

	return buf.Bytes(), nil
}

// DownloadProjectToDirectory downloads and extracts a project to a directory
func (s *WorkflowService) DownloadProjectToDirectory(ctx context.Context, projectID, outputDir string) error {
	return s.DownloadProjectToDirectoryWithRevision(ctx, projectID, "", outputDir)
}

// DownloadProjectToDirectoryWithRevision downloads and extracts a specific revision of a project to a directory
func (s *WorkflowService) DownloadProjectToDirectoryWithRevision(ctx context.Context, projectID, revision, outputDir string) error {
	// Download the project archive
	archiveData, err := s.DownloadProjectWithRevision(ctx, projectID, revision)
	if err != nil {
		return err
	}

	// Extract the archive to the specified directory
	return extractTarGz(archiveData, outputDir)
}

// GetProjectByName retrieves a specific project by name using direct API call
func (s *WorkflowService) GetProjectByName(ctx context.Context, projectName string) (*WorkflowProject, error) {
	// Validate input
	if projectName == "" {
		return nil, NewValidationError("projectName", projectName, "cannot be empty")
	}

	u := fmt.Sprintf("api/projects?name=%s", url.QueryEscape(projectName))

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp WorkflowProjectListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	// Check if we found any projects
	if len(resp.Projects) == 0 {
		return nil, fmt.Errorf("no project found with name: %s", projectName)
	}

	// Return the first matching project
	project := &resp.Projects[0]

	// Validate that we got a valid project with ID
	if project.ID == "" {
		return nil, fmt.Errorf("project found but ID is empty for project: %s", projectName)
	}

	return project, nil
}

// FindProjectByName finds a project by name and returns its ID (deprecated - use GetProjectByName instead)
// This method is kept for backward compatibility but is inefficient as it lists all projects
func (s *WorkflowService) FindProjectByName(ctx context.Context, projectName string) (*WorkflowProject, error) {
	if projectName == "" {
		return nil, NewValidationError("projectName", projectName, "cannot be empty")
	}

	// List all projects
	resp, err := s.ListProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list projects: %w", err)
	}

	// Find project with matching name
	var matchingProjects []*WorkflowProject
	for _, project := range resp.Projects {
		if project.Name == projectName {
			matchingProjects = append(matchingProjects, &project)
		}
	}

	if len(matchingProjects) == 0 {
		return nil, fmt.Errorf("no project found with name: %s", projectName)
	}

	if len(matchingProjects) > 1 {
		return nil, fmt.Errorf("multiple projects found with name: %s (found %d)", projectName, len(matchingProjects))
	}

	return matchingProjects[0], nil
}

// DownloadProjectByNameToDirectory downloads and extracts a project by name to a directory
func (s *WorkflowService) DownloadProjectByNameToDirectory(ctx context.Context, projectName, outputDir string) error {
	return s.DownloadProjectByNameToDirectoryWithRevision(ctx, projectName, "", outputDir)
}

// DownloadProjectByNameToDirectoryWithRevision downloads and extracts a specific revision of a project by name to a directory
func (s *WorkflowService) DownloadProjectByNameToDirectoryWithRevision(ctx context.Context, projectName, revision, outputDir string) error {
	// Get project by name using direct API call
	project, err := s.GetProjectByName(ctx, projectName)
	if err != nil {
		return err
	}

	// Download using the found project ID
	return s.DownloadProjectToDirectoryWithRevision(ctx, project.ID, revision, outputDir)
}
