package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// CreateActivation creates a new activation for a segment
func (s *CDPService) CreateActivation(ctx context.Context, segmentID, name, description string, attributes map[string]interface{}) (*CDPActivation, error) {
	u := fmt.Sprintf("entities/segments/%s/syndications", segmentID)

	// Build the JSON API format request body
	body := map[string]interface{}{
		"type": "syndication",
		"attributes": map[string]interface{}{
			"name":        name,
			"description": description,
		},
	}

	// Merge additional attributes
	for key, value := range attributes {
		body["attributes"].(map[string]interface{})[key] = value
	}

	req, err := s.client.NewCDPRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var response struct {
		Data CDPActivation `json:"data"`
	}
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response.Data, nil
}

// CreateActivationWithRequest creates a new activation using a request struct
func (s *CDPService) CreateActivationWithRequest(ctx context.Context, segmentID string, req *CDPActivationCreateRequest) (*CDPActivation, error) {
	u := fmt.Sprintf("entities/segments/%s/syndications", segmentID)

	// Build the JSON API format request body
	body := map[string]interface{}{
		"type": "syndication",
		"attributes": map[string]interface{}{
			"name":          req.Name,
			"description":   req.Description,
			"type":          req.Type,
			"configuration": req.Configuration,
		},
	}

	// Add optional fields
	if req.SegmentFolderID != nil {
		body["attributes"].(map[string]interface{})["segmentFolderId"] = *req.SegmentFolderID
	}
	if req.AudienceID != nil {
		body["attributes"].(map[string]interface{})["audienceId"] = *req.AudienceID
	}

	request, err := s.client.NewCDPRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var response struct {
		Data CDPActivation `json:"data"`
	}
	_, err = s.client.Do(ctx, request, &response)
	if err != nil {
		return nil, err
	}

	return &response.Data, nil
}

// ListActivations returns a list of activations for a specific audience
func (s *CDPService) ListActivations(ctx context.Context, audienceID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("audiences/%s/syndications", audienceID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// ListSegmentActivations returns a list of activations for a specific segment
func (s *CDPService) ListSegmentActivations(ctx context.Context, segmentID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("entities/segments/%s/syndications", segmentID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var response CDPEntitiesActivationListResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: response.Data,
		Total:       int64(len(response.Data)),
	}, nil
}

// GetActivation retrieves a specific activation (syndication) by ID from an audience segment
func (s *CDPService) GetActivation(ctx context.Context, audienceID, segmentID, activationID string) (*CDPActivation, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s", audienceID, segmentID, activationID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activation CDPActivation
	_, err = s.client.Do(ctx, req, &activation)
	if err != nil {
		return nil, err
	}

	return &activation, nil
}

// UpdateActivationStatus updates the status of an activation (syndication)
func (s *CDPService) UpdateActivationStatus(ctx context.Context, audienceID, segmentID, activationID, status string) (*CDPActivation, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s", audienceID, segmentID, activationID)

	body := map[string]string{
		"status": status,
	}

	req, err := s.client.NewCDPRequest("PATCH", u, body)
	if err != nil {
		return nil, err
	}

	var activation CDPActivation
	_, err = s.client.Do(ctx, req, &activation)
	if err != nil {
		return nil, err
	}

	return &activation, nil
}

// UpdateActivation updates an existing activation
func (s *CDPService) UpdateActivation(ctx context.Context, audienceID, segmentID, activationID string, req *CDPActivationUpdateRequest) (*CDPActivation, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s", audienceID, segmentID, activationID)

	request, err := s.client.NewCDPRequest("PUT", u, req)
	if err != nil {
		return nil, err
	}

	var activation CDPActivation
	_, err = s.client.Do(ctx, request, &activation)
	if err != nil {
		return nil, err
	}

	return &activation, nil
}

// DeleteActivation deletes an activation (syndication) from an audience segment
func (s *CDPService) DeleteActivation(ctx context.Context, audienceID, segmentID, activationID string) error {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s", audienceID, segmentID, activationID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete activation: %s", activationID)
	}

	return nil
}

// ExecuteActivation starts an activation execution for a syndication
func (s *CDPService) ExecuteActivation(ctx context.Context, audienceID, segmentID, activationID string) (*CDPActivationExecution, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s/runs", audienceID, segmentID, activationID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var execution CDPActivationExecution
	_, err = s.client.Do(ctx, req, &execution)
	if err != nil {
		return nil, err
	}

	return &execution, nil
}

// GetActivationExecutions retrieves execution history for a specific activation
func (s *CDPService) GetActivationExecutions(ctx context.Context, audienceID, segmentID, activationID string) ([]CDPActivationExecution, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s/runs", audienceID, segmentID, activationID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var executions []CDPActivationExecution
	_, err = s.client.Do(ctx, req, &executions)
	if err != nil {
		return nil, err
	}

	return executions, nil
}

// GetAudienceActivations retrieves activations for a specific audience
func (s *CDPService) GetAudienceActivations(ctx context.Context, audienceID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("audiences/%s/syndications", audienceID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// GetSegmentFolderActivations retrieves activations for a specific segment folder
func (s *CDPService) GetSegmentFolderActivations(ctx context.Context, segmentFolderID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("segment_folders/%s/activations", segmentFolderID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// RunSegmentActivation runs an activation for a specific segment
func (s *CDPService) RunSegmentActivation(ctx context.Context, segmentID, activationID string) (*CDPActivationExecution, error) {
	u := fmt.Sprintf("entities/segments/%s/activations/%s/run", segmentID, activationID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var execution CDPActivationExecution
	_, err = s.client.Do(ctx, req, &execution)
	if err != nil {
		return nil, err
	}

	return &execution, nil
}

// GetParentSegmentActivations retrieves activations for a parent segment
func (s *CDPService) GetParentSegmentActivations(ctx context.Context, parentSegmentID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("entities/parent_segments/%s/activations", parentSegmentID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// GetAudienceFolderSyndications retrieves syndications for a specific audience folder
func (s *CDPService) GetAudienceFolderSyndications(ctx context.Context, audienceID, folderID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("audiences/%s/folders/%s/syndications", audienceID, folderID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// GetSegmentSyndications retrieves syndications for a specific segment
func (s *CDPService) GetSegmentSyndications(ctx context.Context, audienceID, segmentID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications", audienceID, segmentID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// GetParentSegmentUserDefinedWorkflowProjects retrieves user-defined workflow projects for a parent segment
func (s *CDPService) GetParentSegmentUserDefinedWorkflowProjects(ctx context.Context, parentSegmentID string) (*CDPUserDefinedWorkflowProjectListResponse, error) {
	u := fmt.Sprintf("entities/parent_segments/%s/user_defined_workflow_projects", parentSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	// Handle JSON API format response
	var jsonAPIResp struct {
		Data []struct {
			ID         string `json:"id"`
			Type       string `json:"type"`
			Attributes struct {
				Name string `json:"name"`
			} `json:"attributes"`
		} `json:"data"`
	}
	_, err = s.client.Do(ctx, req, &jsonAPIResp)
	if err != nil {
		return nil, err
	}

	// Convert to expected format
	var projects []CDPUserDefinedWorkflowProject
	for _, item := range jsonAPIResp.Data {
		projects = append(projects, CDPUserDefinedWorkflowProject{
			ID:   item.ID,
			Name: item.Attributes.Name,
		})
	}

	return &CDPUserDefinedWorkflowProjectListResponse{
		Projects: projects,
		Total:    int64(len(projects)),
	}, nil
}

// GetParentSegmentUserDefinedWorkflows retrieves user-defined workflows for a parent segment
func (s *CDPService) GetParentSegmentUserDefinedWorkflows(ctx context.Context, parentSegmentID, workflowProjectName string) (*CDPUserDefinedWorkflowListResponse, error) {
	u := fmt.Sprintf("entities/parent_segments/%s/user_defined_workflows", parentSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	// Add required workflowProjectName query parameter
	q := req.URL.Query()
	q.Set("workflowProjectName", workflowProjectName)
	req.URL.RawQuery = q.Encode()

	// Handle JSON API format response
	var jsonAPIResp struct {
		Data []struct {
			ID         string `json:"id"`
			Type       string `json:"type"`
			Attributes struct {
				Name string `json:"name"`
			} `json:"attributes"`
		} `json:"data"`
	}
	_, err = s.client.Do(ctx, req, &jsonAPIResp)
	if err != nil {
		return nil, err
	}

	// Convert to expected format
	var workflows []CDPUserDefinedWorkflow
	for _, item := range jsonAPIResp.Data {
		workflows = append(workflows, CDPUserDefinedWorkflow{
			ID:   item.ID,
			Name: item.Attributes.Name,
		})
	}

	return &CDPUserDefinedWorkflowListResponse{
		Workflows: workflows,
		Total:     int64(len(workflows)),
	}, nil
}

// GetParentSegmentMatchedActivations retrieves matched activations for a parent segment
func (s *CDPService) GetParentSegmentMatchedActivations(ctx context.Context, parentSegmentID string) (*CDPMatchedActivationListResponse, error) {
	u := fmt.Sprintf("entities/parent_segments/%s/matched_activations", parentSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPMatchedActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPMatchedActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}
