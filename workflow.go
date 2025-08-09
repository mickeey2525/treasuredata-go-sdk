package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)




// WorkflowService handles communication with the Workflow related methods of the Treasure Data API.
type WorkflowService struct {
	client *Client
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





// WorkflowListOptions specifies optional parameters to Workflow List method
type WorkflowListOptions struct {
	Limit  int `url:"limit,omitempty"`
	Offset int `url:"offset,omitempty"`
}


// WorkflowListResponse represents the response from the workflow list API
type WorkflowListResponse struct {
	Workflows []Workflow `json:"workflows"`
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


