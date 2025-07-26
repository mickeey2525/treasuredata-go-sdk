package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// GetEntitiesByFolder retrieves entities that belong to a specific folder using JSON:API format
func (s *CDPService) GetEntitiesByFolder(ctx context.Context, folderID string) (*CDPJSONAPIListResponse, error) {
	u := fmt.Sprintf("entities/by-folder/%s", folderID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJSONAPIListResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// NewEntityFolderCreateRequest creates a JSON API request body for entity folder creation
func NewEntityFolderCreateRequest(name, description string, parentFolderID *string) *CDPEntityFolderCreateRequest {
	req := &CDPEntityFolderCreateRequest{
		Type: "folder-segment",
		Attributes: CDPEntityFolderCreateAttributes{
			Name:        name,
			Description: description,
		},
	}

	if parentFolderID != nil && *parentFolderID != "" {
		req.Relationships = &CDPEntityFolderCreateRelationships{
			ParentFolder: &CDPEntityFolderParentData{
				Data: &CDPEntityFolderParentInfo{
					ID:   *parentFolderID,
					Type: "folder-segment",
				},
			},
		}
	}

	return req
}

// CreateEntityFolderWithParams creates a new entity folder with direct parameters
func (s *CDPService) CreateEntityFolderWithParams(ctx context.Context, name, description string, parentFolderID *string) (*CDPFolder, error) {
	req := &CDPFolderCreateRequest{
		Name:        name,
		Description: description,
		ParentID:    parentFolderID,
	}
	return s.CreateEntityFolder(ctx, req)
}

// CreateEntityFolder creates a new entity folder using JSON API format
func (s *CDPService) CreateEntityFolder(ctx context.Context, req *CDPFolderCreateRequest) (*CDPFolder, error) {
	u := "entities/folders"

	// Convert to JSON API format
	jsonAPIReq := NewEntityFolderCreateRequest(req.Name, req.Description, req.ParentID)

	request, err := s.client.NewCDPRequest("POST", u, jsonAPIReq)
	if err != nil {
		return nil, err
	}

	var folder CDPFolder
	_, err = s.client.Do(ctx, request, &folder)
	if err != nil {
		return nil, err
	}

	return &folder, nil
}

// GetEntityFolder retrieves a specific entity folder by ID using JSON:API format
func (s *CDPService) GetEntityFolder(ctx context.Context, folderID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/folders/%s", folderID)

	req, err := s.client.NewCDPJSONAPIRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJSONAPIResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// UpdateEntityFolder updates an existing entity folder using JSON API format
func (s *CDPService) UpdateEntityFolder(ctx context.Context, folderID string, req *CDPFolderUpdateRequest) (*CDPFolder, error) {
	u := fmt.Sprintf("entities/folders/%s", folderID)

	// Convert to JSON API format
	jsonAPIReq := map[string]interface{}{
		"data": map[string]interface{}{
			"id":   folderID,
			"type": "folder-segment",
			"attributes": map[string]interface{}{
				"name":        req.Name,
				"description": req.Description,
			},
		},
	}

	request, err := s.client.NewCDPRequest("PATCH", u, jsonAPIReq)
	if err != nil {
		return nil, err
	}

	var folder CDPFolder
	_, err = s.client.Do(ctx, request, &folder)
	if err != nil {
		return nil, err
	}

	return &folder, nil
}

// DeleteEntityFolder deletes an entity folder
func (s *CDPService) DeleteEntityFolder(ctx context.Context, folderID string) error {
	u := fmt.Sprintf("entities/folders/%s", folderID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete entity folder: %s", folderID)
	}

	return nil
}
